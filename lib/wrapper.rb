require 'aws-sdk'
require 'zlib'
require 'csv'

class Segment

    def initialize(options)

        @config = JSON.parse(File.read(options[:data] + '/config.json'))
        @out_bucket = @config["parameters"]["outputbucket"]
        @kbc_api_token = ENV["KBC_TOKEN"]

        $out_file = options[:data] + '/out/tables/' + 'status.csv'

    end

    def download()

      s3 = Aws::S3::Resource.new(
        access_key_id: '',
        secret_access_key: '',
        region: ''
      )

      client = Aws::S3::Client.new(
        access_key_id: '',
        secret_access_key: '',
        region: ''
      )

      data_files = s3.bucket('').objects(prefix: '', delimiter: '').collect(&:key)

      data_files.each { |key|

      puts key
      reap = client.get_object({ bucket: '', key: key }, target: 'file.gz')

      Zlib::GzipReader.open('file.gz') do | input_stream |
        File.open("file.csv", "a", :quote_char => '|') do |output_stream|
          IO.copy_stream(input_stream, output_stream)
        end
      end

      }

      CSV.open('out.csv', "ab", :col_sep => '|') do |header|
          header << ["data"]
      end

      CSV.foreach('file.csv', :encoding => 'utf-8', :quote_char => '`', :col_sep => '|') do |row|

        CSV.open('out.csv', "ab") do |rows|
            rows << row
        end

      end

    end

end
