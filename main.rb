require './lib/wrapper'

options = {}
OptionParser.new do |opts|

  opts.on('-d', '--data DAT', 'Data') {|v| options[:data] = v}

end.parse!

if options[:data].nil?
then
  puts 'No data folder is set.'
  exit 1
end

extractor = Segment.new(options)

if extractor.download == true
then
  puts 'Data downloaded.'
  exit 0
else
  exit 1
end
