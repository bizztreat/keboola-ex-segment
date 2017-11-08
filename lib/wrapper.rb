require 'aws-sdk'
require 'zlib'
require 'csv'
require 'optparse'
require 'date'

class Segment

  def initialize(options)

    #@config = JSON.parse(File.read('local/config-trial.json'))
    @config = JSON.parse(File.read(options[:data] + '/config.json'))
    @out_bucket = @config['parameters']['outputbucket']
    @s3_bucket = @config['parameters']['s3_bucket']
    @s3_prefix = @config['parameters']['s3_prefix']
    @access_key = @config['parameters']['#access_key']
    @secret_access_key = @config['parameters']['#secret_access_key']
    @region = @config['parameters']['region']
    @olderThan = @config['parameters']['changed_in_last_days'] # days
    @countOk = 0
    @countSkip = 0

    puts '* Version 1.1.2'
    if @olderThan.nil? || @olderThan == '0'
    then
      puts '* All files.'
    else
      @olderThanLimit = Time.now - (60 * 60 * 24) * Integer(@olderThan)
      puts "* Files changed after (#{@olderThanLimit})"
    end

    @in_file = options[:data] + '/in/tables/file.gz'
    @in_file_decompressed = options[:data] + '/in/tables/file.csv'
    @out_file = options[:data] + '/out/tables/out.csv'

    #@in_file = 'local/in/tables/file.gz'
    #@in_file_decompressed = 'local/in/tables/file.csv'
    #@out_file = 'local/out/tables/out.csv'

    @kbc_api_token = ENV['KBC_TOKEN']

  end

  def download()

    s3 = Aws::S3::Resource.new(
        access_key_id: @access_key,
        secret_access_key: @secret_access_key,
        region: @region
    )

    client = Aws::S3::Client.new(
        access_key_id: @access_key,
        secret_access_key: @secret_access_key,
        region: @region
    )

    data_files = s3.bucket(@s3_bucket).objects(prefix: @s3_prefix, delimiter: '').collect(&:key)

    data_files.each {|key|

      if !key.include? '.gz'
        puts "* Folder * #{key}"
        next
      end

      reap = client.get_object({bucket: @s3_bucket, key: key}, target: @in_file)

      if @olderThanLimit.nil? || reap.last_modified >= @olderThanLimit
      then
        #puts "Procesing ... (#{reap.last_modified}) #{key} "
        Zlib::GzipReader.open(@in_file) do |input_stream|
          File.open(@in_file_decompressed, 'a', :quote_char => '∑', :col_sep => '|') do |output_stream|
            IO.copy_stream(input_stream, output_stream)
          end
        end
        @countOk += 1
      else
        #puts "Old ... (#{reap.last_modified}) #{key} "
        @countSkip += 1
      end

    }

    puts "* New files processed: #{@countOk}"
    puts "* Old files skipped: #{@countSkip}"

    CSV.open(@out_file, 'ab', :col_sep => '|') do |header|
      header << ['data']
    end

    puts '* Writing data to the output. It may take a minutes!'

    CSV.foreach(@in_file_decompressed, :encoding => 'utf-8', :quote_char => '∑', :col_sep => '|') do |row|

      msg = ''
      r = row
      if r.to_s.include? 'clearbit_'
      then
        msg = 'clearbit row escaped'
      else
        if r.to_s.include? 'Features search used'
        then
          msg = 'problematic event escaped'
        else
          if r.to_s.include? 'assign to release'
          then
            msg = 'problematic event escaped'
          else
            if r.to_s.include? "\", \" "
            then
              msg = 'problematic event skipped'
            else
              CSV.open(@out_file, 'ab', :encoding => 'utf-8') do |rows|
                rows << row
              end
            end
          end
        end
      end

      ## Info message
      #if msg.to_s.strip.empty?
      #  puts "Warning: #{msg}"
      #end

    end

    return true

  end

end
