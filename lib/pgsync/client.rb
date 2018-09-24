require 'table_print'

module PgSync
  class Client
    def initialize(args)
      $stdout.sync = true
      $stderr.sync = true
      @exit = false
      @arguments, @options = parse_args(args)
      @mutex = windows? ? Mutex.new : MultiProcessing::Mutex.new
    end

    # TODO clean up this mess
    def perform
      return if @exit

      args, opts = @arguments, @options
      [:to, :from, :to_safe, :exclude, :schemas].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end
      map_deprecations(args, opts)

      if opts[:init]
        setup(db_config_file(args[0]) || config_file || ".pgsync.yml")
      else
        sync(args, opts)
      end

      true
    end

    protected

    def sync(args, opts)
      start_time = Time.now

      if args.size > 2
        raise PgSync::Error, "Usage:\n    pgsync [options]"
      end

      timeout = 5
      if opts[:timeout].nil?
        timeout = opts[:timeout].to_i
      end

      source = DataSource.new('SRC',opts[:from],opts[:debug],timeout)
      raise PgSync::Error, "No source" unless source.exists?

      destination = DataSource.new('DEST',opts[:to],opts[:debug],timeout)
      raise PgSync::Error, "No destination" unless destination.exists?

      begin
        # start connections
        source.host
        destination.host

        unless opts[:to_safe] || destination.local?
          raise PgSync::Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
        end

        print_description("From", source)
        print_description("To", destination)
      ensure
        source.close
        destination.close
      end

      tables = nil
      begin
        tables = TableList.new(args, opts, source, config).tables
      ensure
        source.close
      end

      confirm_tables_exist(source, tables, "source")

      if opts[:list]
        confirm_tables_exist(destination, tables, "destination")

        list_items =
          if args[0] == "groups"
            (config["groups"] || {}).keys
          else
            tables.keys
          end

        pretty_list list_items
      elsif opts[:list_tables]
        puts "Listing tables."

        no_changes_tables = []
        rails_sync_tables = []
        sync_tables = []
        full_sync_tables = [] 
        error_tables = []

        # rename_tables = config["rename_tables"]
        # if rename_tables.nil?
        #   rename_tables = {}
        # end
        # pp rename_tables
          
        tables.each do |table|
          sync_table = table[0]

          from_table = sync_table
          to_table = destination_table(from_table)

          puts "from_table #{from_table.to_s},to_table #{to_table.to_s}" if opts[:debug]
          begin
            to_table_count = destination.count_table(to_table)
            from_table_count = source.count_table(from_table)

            has_primary_key = !source.primary_key(from_table).nil?
            rails_table = destination.is_rails_table?(to_table) 

            # puts "has_primary_key #{has_primary_key}"

            pk_desc = "NO PK"
            if (has_primary_key)
              pk_desc = "WITH PK"
            end

            table_diff = ""
            if to_table_count > from_table_count
              table_diff = "***TRUNCATE REQUIRED - #{pk_desc}***".bold.red
              full_sync_tables << [from_table,from_table_count]
            elsif to_table_count < from_table_count
              if (rails_table && has_primary_key)
                table_diff = "<<<RAILS SYNC>>>".blue
                rails_sync_tables << [from_table,from_table_count]
              elsif (!has_primary_key)
                table_diff = "***FULL SYNC - #{pk_desc}***".bold.red
                full_sync_tables << [from_table,from_table_count]
              else
                table_diff = "<<<NORMAL SYNC>>>".brown
                sync_tables << [from_table,from_table_count]
              end
            else
              table_diff = "NO CHANGE".green
              no_changes_tables << [from_table,from_table_count]
            end

            rails_desc = ""
            if (rails_table)
              rails_desc = "[Rails]".blue
            else
              rails_desc = "[NOT Rails]"
            end

            log "* Info #{from_table} \t\t\t\tSRC:#{from_table_count} \t\tDEST:#{to_table_count} \t\t#{table_diff} \t\t#{rails_desc}"
          rescue SignalException => e
            raise e                  
          rescue Exception => exc
            puts exc.message
            puts "Skipping table #{from_table}"
            error_tables << from_table
          end
        end

        puts "-------------------------------------"
        puts "Tables in Sync: #{no_changes_tables.sort_by(&:last).map(&:first).join(",")}"
        puts "-------------------------------------"
        puts "Rails Sync Tables: pgsync --rails --preserve --in-batches #{rails_sync_tables.sort_by(&:last).map(&:first).join(",")}"
        puts "-------------------------------------"
        puts "Normal PGSync Sync Tables:  pgsync --rails --preserve --in-batches #{sync_tables.sort_by(&:last).map(&:first).join(",")}"
        puts "-------------------------------------"
        puts "Truncate & Sync Tables: pgsync #{full_sync_tables.sort_by(&:last).map(&:first).join(",")}"    
        puts " size_info #{full_sync_tables.sort_by(&:last).map{|i| i.join("=")}.join(",")}"    
        puts "-------------------------------------"
        puts "Error Tables: #{error_tables.join(",")}"    
        puts "-------------------------------------"

      elsif opts[:activity]
        timeout = 5
        if opts[:timeout].nil?
          timeout = opts[:timeout].to_i
        end

        source = DataSource.new('SRC',opts[:from],opts[:debug],timeout)
        raise PgSync::Error, "No source" unless source.exists?

        # destination = DataSource.new(opts[:to])
        # raise PgSync::Error, "No destination" unless destination.exists?

        sql = "SELECT * FROM pg_stat_activity WHERE query NOT ILIKE '%pg_stat_activity%'"
        result = source.execute(sql)
        tp result
      else
        if opts[:schema_first] || opts[:schema_only]
          if opts[:preserve]
            raise PgSync::Error, "Cannot use --preserve with --schema-first or --schema-only"
          end

          log "* Dumping schema"
          sync_schema(source, destination, tables)
        end

        unless opts[:schema_only]
          confirm_tables_exist(destination, tables, "destination")

          in_parallel(tables) do |table, table_opts|
            TableSync.new.sync(@mutex, config, table, opts.merge(table_opts), source.url, destination.url, source.search_path.find { |sp| sp != "pg_catalog" })
          end
        end

        log_completed(start_time)
      end
    end

    def confirm_tables_exist(data_source, tables, description)
      tables.keys.each do |table|
        t = table
        
        if (description == "destination")
          t = destination_table(t)
        end

        unless data_source.table_exists?(t)
          raise PgSync::Error, "Table does not exist in #{description}: #{t}"
        end
      end
    ensure
      data_source.close
    end

    def map_deprecations(args, opts)
      command = args[0]

      case command
      when "setup"
        args.shift
        opts[:init] = true
        deprecated "Use `psync --init` instead"
      when "schema"
        args.shift
        opts[:schema_only] = true
        deprecated "Use `psync --schema-only` instead"
      when "tables"
        args.shift
        opts[:tables] = args.shift
        deprecated "Use `pgsync #{opts[:tables]}` instead"
      when "groups"
        args.shift
        opts[:groups] = args.shift
        deprecated "Use `pgsync #{opts[:groups]}` instead"
      end

      if opts[:where]
        opts[:sql] ||= String.new
        opts[:sql] << " WHERE #{opts[:where]}"
        deprecated "Use `\"WHERE #{opts[:where]}\"` instead"
      end

      if opts[:limit]
        opts[:sql] ||= String.new
        opts[:sql] << " LIMIT #{opts[:limit]}"
        deprecated "Use `\"LIMIT #{opts[:limit]}\"` instead"
      end
    end

    def sync_schema(source, destination, tables)
      dump_command = source.dump_command(tables)
      restore_command = destination.restore_command
      system("#{dump_command} | #{restore_command}")
    end

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = %{Usage:
    pgsync [options]

Options:}
        o.string "-d", "--db", "database"
        o.string "-t", "--tables", "tables to sync"
        o.string "-g", "--groups", "groups to sync"
        o.string "--schemas", "schemas to sync"
        o.string "--from", "source"
        o.string "--to", "destination"
        o.string "--where", "where", help: false
        o.integer "--limit", "limit", help: false
        o.string "--exclude", "exclude tables"
        o.string "--config", "config file"
        # TODO much better name for this option
        o.boolean "--to-safe", "accept danger", default: false
        o.boolean "--debug", "debug", default: false
        o.boolean "--list", "list", default: false
        o.boolean "--list-tables", "list tables", default: false
        o.boolean "--activity", "show activity", default: false
        o.boolean "--overwrite", "overwrite existing rows", default: false, help: false
        o.boolean "--preserve", "preserve existing rows", default: false
        o.boolean "--truncate", "truncate existing rows", default: false
        o.boolean "--schema-first", "schema first", default: false
        o.boolean "--schema-only", "schema only", default: false
        o.boolean "--all-schemas", "all schemas", default: false
        o.boolean "--no-rules", "do not apply data rules", default: false
        o.boolean "--init", "init", default: false
        o.boolean "--setup", "setup", default: false, help: false
        o.boolean "--in-batches", "in batches", default: false, help: false
        o.integer "--batch-size", "batch size", default: 10000, help: false
        o.integer "--timeout", "timeout", default: 5, help: false
        o.boolean "--ignore-same-size", "ignore tables with same size", default: false
        o.boolean "--rails", "use rails format. if it has a updated_at colum then use it", default: false
        o.boolean "--copy-sequences", "copy sequences", default: false
        o.float "--sleep", "sleep", default: 0, help: false
        o.on "-v", "--version", "print the version" do
          log PgSync::VERSION
          @exit = true
        end
        o.on "-h", "--help", "prints help" do
          log o
          @exit = true
        end
      end

      opts_hash = opts.to_hash
      opts_hash[:init] = opts_hash[:setup] if opts_hash[:setup]

      [opts.arguments, opts_hash]
    rescue Slop::Error => e
      raise PgSync::Error, e.message
    end

    def config
      @config ||= begin
        if config_file
          begin
            YAML.load_file(config_file) || {}
          rescue Psych::SyntaxError => e
            raise PgSync::Error, e.message
          end
        else
          {}
        end
      end
    end

    def destination_table(table)
      rename_tables = config["rename_tables"]
      if rename_tables.nil?
        rename_tables = {}
      end
      # pp rename_tables

      to_table = rename_tables[table]
      if to_table.nil?
        to_table = table
      end      
      to_table
    end

    def setup(config_file)
      if File.exist?(config_file)
        raise PgSync::Error, "#{config_file} exists."
      else
        FileUtils.cp(File.dirname(__FILE__) + "/../../config.yml", config_file)
        log "#{config_file} created. Add your database credentials."
      end
    end

    def db_config_file(db)
      return unless db
      ".pgsync-#{db}.yml"
    end

    def print_description(prefix, source)
      location = " on #{source.host}:#{source.port}" if source.host
      log "#{prefix}: #{source.dbname}#{location}"
    end

    def search_tree(file)
      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        if File.exist?(absolute_file)
          break absolute_file
        end
        path = File.dirname(path)
        break if path == "/"
      end
    end

    def config_file
      return @config_file if instance_variable_defined?(:@config_file)

      @config_file =
        search_tree(
          if @options[:db]
            db_config_file(@options[:db])
          else
            @options[:config] || ".pgsync.yml"
          end
        )
    end

    def log(message = nil)
      $stderr.puts message
    end

    def in_parallel(tables, &block)
      if @options[:debug] || @options[:in_batches]
        tables.each(&block)
      else
        options = {}
        options[:in_threads] = 4 if windows?
        Parallel.each(tables, options, &block)
      end
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
    end

    def deprecated(message)
      log "[DEPRECATED] #{message}"
    end

    def log_completed(start_time)
      time = Time.now - start_time
      log "Completed in #{time.round(1)}s"
    end

    def windows?
      Gem.win_platform?
    end
  end
end
