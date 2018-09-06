require 'pp'

module PgSync
  class TableSync
    def sync(mutex, config, table, opts, source_url, destination_url, first_schema)
      start_time = Time.now
      source = DataSource.new('SRC',source_url, opts[:debug],timeout: 0)
      destination = DataSource.new('DEST',destination_url, opts[:debug], timeout: 0)

      begin
        from_connection = source.conn
        to_connection = destination.conn

        btm = PG::BasicTypeMapForResults.new(from_connection)

        bad_fields = opts[:no_rules] ? [] : config["data_rules"]

        from_fields = source.columns(table)
        to_fields = destination.columns(table)

        includes_updated_at = source.is_rails_table?(table)

        shared_fields = to_fields & from_fields
        extra_fields = to_fields - from_fields
        missing_fields = from_fields - to_fields

        from_sequences = source.sequences(table, shared_fields)
        to_sequences = destination.sequences(table, shared_fields)
        shared_sequences = to_sequences & from_sequences
        extra_sequences = to_sequences - from_sequences
        missing_sequences = from_sequences - to_sequences

        sql_clause = String.new

        table_name = table.sub("#{first_schema}.", "")

        mutex.synchronize do
          log "* Syncing #{table_name}"
          if opts[:sql]
            log "    #{opts[:sql]}"
            sql_clause << " #{opts[:sql]}"
          end
          log "    Extra columns: #{extra_fields.join(", ")}" if extra_fields.any?
          log "    Missing columns: #{missing_fields.join(", ")}" if missing_fields.any?
          log "    Extra sequences: #{extra_sequences.join(", ")}" if extra_sequences.any?
          log "    Missing sequences: #{missing_sequences.join(", ")}" if missing_sequences.any?

          if shared_fields.empty?
            log "    No fields to copy"
          end
        end

        if shared_fields.any?
          # puts "shared_fields"
          # pp shared_fields

          valid_copy_fields_a = shared_fields

          copy_fields_a = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, _| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f)} AS #{quote_ident(f)}" : "#{quote_ident_full(table)}.#{quote_ident(f)}" }
          copy_fields = copy_fields_a.join(", ")
          fields = shared_fields.map { |f| quote_ident(f) }.join(", ")

          seq_values = {}
          shared_sequences.each do |seq|
            seq_values[seq] = source.last_value(seq)
          end

          copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident_full(table)}#{sql_clause}) TO STDOUT"
          if opts[:in_batches]
            raise PgSync::Error, "Cannot use --overwrite with --in-batches" if opts[:overwrite]

            if opts[:ignore_same_size]
              begin
                to_table_count = destination.count_table(table)
                from_table_count = source.count_table(table)

                puts "to_table_count: #{to_table_count} , from_table_count: #{from_table_count}"
                if (to_table_count > from_table_count)
                  if opts[:preserve]
                     puts "Skipping #{table} as truncate is required. FIX MANUALLY"
                     return
                  else
                    puts "truncating #{table}. to table should not exceed from table"
                    destination.truncate(table)
                  end
                elsif (from_table_count == to_table_count)
                  puts "Skipping table #{table}"
                  return
                end
              rescue Exception => exc
                puts exc.message
                puts "Failed to do quick check. Doing normal sync"
              end
            end            

            primary_key = source.primary_key(table)
            raise PgSync::Error, "No primary key" unless primary_key

            destination.truncate(table) if opts[:truncate]

            if (includes_updated_at)
              source_max_updated_at = source.max_updated_at(table)
              destination_max_updated_at = destination.max_updated_at(table)

              puts "source_max_updated_at #{source_max_updated_at}"
              puts "destination_max_updated_at #{destination_max_updated_at}"
            end

            puts "primary_key #{primary_key}"

            destination_max_updated_at = destination.max_updated_at(table) if includes_updated_at

            if opts[:rails] && includes_updated_at && !opts[:truncate] && destination_max_updated_at > 0
              source_max_updated_at = source.max_updated_at(table)
              
              if source_max_updated_at == destination_max_updated_at
                puts "RAILS TABLE IN SYNC. SKIPPING"
                return
              else
                sql_command = "SELECT #{copy_fields} FROM #{quote_ident_full(table)} where extract(epoch from updated_at) >= #{destination_max_updated_at} "     

                res = from_connection.exec(sql_command)
                if (!btm.nil?)
                  res.type_map = btm
                end
                pp res
                puts "Effected row count: #{res.cmd_tuples}"
                
                res.each do |row|
                  insert_sql = """
                      INSERT INTO #{table} (#{valid_copy_fields_a.join(",")}) 
                      VALUES (#{bind_vars(valid_copy_fields_a)})
                      ON CONFLICT (#{primary_key}) DO UPDATE 
                        SET #{conflict_setters(primary_key,valid_copy_fields_a)};
                  """
                  # puts "insert_sql:#{insert_sql}"
                  to_connection.exec_params(insert_sql , format_values(valid_copy_fields_a,row))
                  # puts "inserted...."
                end       
              end        
            else
              from_id = source.max_id(table, primary_key)
              to_id = destination.max_id(table, primary_key) + 1
            
              if to_id == 1
                from_min_id = source.min_id(table, primary_key)
                to_id = from_min_id if from_min_id > 0
              end

              starting_id = to_id
              batch_size = opts[:batch_size]

              i = 1
              batch_count = ((from_id - starting_id + 1) / batch_size.to_f).ceil

              puts "starting_id: #{starting_id}"
              puts "from_id: #{from_id}"

              while starting_id <= from_id
                where = "#{quote_ident(primary_key)} >= #{starting_id} AND #{quote_ident(primary_key)} < #{starting_id + batch_size}"
                log "    #{i}/#{batch_count}: #{where}"

                # TODO be smarter for advance sql clauses
                batch_sql_clause = " #{sql_clause.length > 0 ? "#{sql_clause} AND" : "WHERE"} #{where}"

                batch_sql_copy_command = "SELECT #{copy_fields} FROM #{quote_ident_full(table)}#{batch_sql_clause}"

                batch_copy_to_command = "COPY (#{batch_sql_copy_command}) TO STDOUT"

                puts "batch_copy_to_command: #{batch_copy_to_command}"
                to_sql = "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN"
                puts "to_sql: #{to_sql}"

                to_connection.copy_data(to_sql) do
                  from_connection.copy_data(batch_copy_to_command) do
                    while (row = from_connection.get_copy_data)
                        to_connection.put_copy_data(row)
                    end
                  end
                end

                starting_id += batch_size
                i += 1

                if opts[:sleep] && starting_id <= from_id
                  sleep(opts[:sleep])
                end
              end
            end
            
          elsif !opts[:truncate] && (opts[:overwrite] || opts[:preserve] || !sql_clause.empty?)
            primary_key = destination.primary_key(table)
            raise PgSync::Error, "No primary key" unless primary_key

            temp_table = "pgsync_#{rand(1_000_000_000)}"
            file = Tempfile.new(temp_table)
            begin
              from_connection.copy_data copy_to_command do
                while (row = from_connection.get_copy_data)
                  file.write(row)
                end
              end
              file.rewind

              # create a temp table
              to_connection.exec("CREATE TEMPORARY TABLE #{quote_ident_full(temp_table)} AS SELECT * FROM #{quote_ident_full(table)} WITH NO DATA")

              # load file
              to_connection.copy_data "COPY #{quote_ident_full(temp_table)} (#{fields}) FROM STDIN" do
                file.each do |row|
                  to_connection.put_copy_data(row)
                end
              end

              if opts[:preserve]
                # insert into
                to_connection.exec("INSERT INTO #{quote_ident_full(table)} (SELECT * FROM #{quote_ident_full(temp_table)} WHERE NOT EXISTS (SELECT 1 FROM #{quote_ident_full(table)} WHERE #{quote_ident_full(table)}.#{quote_ident(primary_key)} = #{quote_ident_full(temp_table)}.#{quote_ident(primary_key)}))")
              else
                to_connection.transaction do
                  to_connection.exec("DELETE FROM #{quote_ident_full(table)} WHERE #{quote_ident(primary_key)} IN (SELECT #{quote_ident(primary_key)} FROM #{quote_ident_full(temp_table)})")
                  to_connection.exec("INSERT INTO #{quote_ident_full(table)} (SELECT * FROM #{quote_ident(temp_table)})")
                end
              end
            ensure
               file.close
               file.unlink
            end
          elsif opts[:ignore_same_size]
            to_table_count = destination.count_table(table)
            from_table_count = source.count_table(table)
            puts "to_table_count: #{to_table_count} , from_table_count: #{from_table_count}"
            if (from_table_count != to_table_count)
              destination.truncate(table)
              to_connection.copy_data "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN" do
                from_connection.copy_data copy_to_command do
                  while (row = from_connection.get_copy_data)
                    to_connection.put_copy_data(row)
                  end
                end
              end
            else
              puts "Skipping table #{table} as row counts are the same."
            end            
          else
            destination.truncate(table)
            to_connection.copy_data "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN" do
              from_connection.copy_data copy_to_command do
                while (row = from_connection.get_copy_data)
                  to_connection.put_copy_data(row)
                end
              end
            end
          end
          seq_values.each do |seq, value|
            to_connection.exec("SELECT setval(#{escape(seq)}, #{escape(value)})")
          end
        end
        mutex.synchronize do
          log "* DONE #{table_name} (#{(Time.now - start_time).round(1)}s)"
        end
      ensure
        source.close
        destination.close
      end
    end

    private

    def bind_vars(fields)
      fields.each_with_index.map { |e,i| "$#{i+1}"}.join(",")
    end

    def format_values(fields,row)
      # puts "format_values"
      # pp fields
      # pp row
      # t.strftime("%Y-%m-%d %H:%M:%S.%6N %Z")
       # => "2016-10-12 12:07:20.967813 CEST" 
      # row.values

      fields.map{ |f| 
        if ((f == "updated_at" || f == "created_at") && !row[f].nil?)
          row[f].strftime("%Y-%m-%d %H:%M:%S.%6N %Z")
        else
          row[f]
        end
      }
    end

    def conflict_setters(primary_key,fields)
      setter_fields = fields.select{|i| i != primary_key}
      setter_fields.map{ |f| " #{f} = excluded.#{f} "}.join(",")
    end

    # market_pair_id = excluded.market_pair_id, 
    #                         market_id = excluded.market_id,
    #                         message = excluded.message,
    #                         created_at = excluded.created_at,
    #                         updated_at = excluded.updated_at

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table.split(".", 2)[-1]}.#{column}") || regex.match("#{table}.#{column}")
    end

    # TODO wildcard rules
    def apply_strategy(rule, table, column)
      if rule.is_a?(Hash)
        if rule.key?("value")
          escape(rule["value"])
        elsif rule.key?("statement")
          rule["statement"]
        else
          raise PgSync::Error, "Unknown rule #{rule.inspect} for column #{column}"
        end
      else
        strategies = {
          "unique_email" => "'email' || #{table}.id || '@example.org'",
          "untouched" => quote_ident(column),
          "unique_phone" => "(#{table}.id + 1000000000)::text",
          "random_int" => "(RAND() * 10)::int",
          "random_date" => "'1970-01-01'",
          "random_time" => "NOW()",
          "unique_secret" => "'secret' || #{table}.id",
          "random_ip" => "'127.0.0.1'",
          "random_letter" => "'A'",
          "random_string" => "right(md5(random()::text),10)",
          "random_number" => "(RANDOM() * 1000000)::int",
          "null" => "NULL",
          nil => "NULL"
        }
        if strategies[rule]
          strategies[rule]
        else
          raise PgSync::Error, "Unknown rule #{rule} for column #{column}"
        end
      end
    end

    def log(message = nil)
      $stderr.puts message
    end

    def quote_ident_full(ident)
      ident.split(".").map { |v| quote_ident(v) }.join(".")
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def escape(value)
      if value.is_a?(String)
        "'#{quote_string(value)}'"
      else
        value
      end
    end

    # activerecord
    def quote_string(s)
      s.gsub(/\\/, '\&\&').gsub(/'/, "''")
    end
  end
end
