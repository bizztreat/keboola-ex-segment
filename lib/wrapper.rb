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
    @tSumGet = 0
    @tSumZip = 0

    puts '* Version 1.1.3'
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

    t0 = Time.now
    puts "Start time: #{t0}"

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

    t1 = Time.now
    puts "T1 After list: #{t1 - t0}s"

    data_files.each {|key|

      if !key.include? '.gz'
        puts "* Folder * #{key}"
        next
      end

      t11 = Time.now

      reap = client.get_object({bucket: @s3_bucket, key: key}, target: @in_file)

      t12 = Time.now
      #puts "T11 After get Obj: #{t12 - t11}s"
      @tSumGet += (t12 - t11)

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

      t13 = Time.now
      #puts "T11 After Obj unzip: #{t13 - t12}s"
      @tSumZip += (t13 - t12)

    }

    t2 = Time.now
    puts "T2 After unzip: #{t2 - t1}s"
    puts "* SUM Time to get objects: #{@tSumGet}s"
    puts "* SUM Time to unzip objects: #{@tSumZip}s"

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

    t3 = Time.now
    puts "T3 After output: #{t3 - t2}s"

    puts "End time: #{t3}"
    puts "Total time: #{t3 - t0}s"


    return true

  end

end
