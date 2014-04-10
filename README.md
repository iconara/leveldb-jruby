# LevelDB for JRuby

## Installation

Add this line to your application's Gemfile:

    gem 'leveldb-jruby', require: 'leveldb'

## Usage

```ruby
db = LevelDb.open('path/to/database')

# basic key operations
db.put('foo', 'bar')
puts db.get('foo') # => 'bar'
db.delete('foo')

# iterating over a range of keys
10.times { |i| db.put("foo#{i.to_s.rjust(2, '0')}", i.to_s) }
db.each(from: 'foo', to: 'foo08') do |key, value|
  puts "#{key} => #{value}"
end

# batch mutations
db.batch do |batch|
  batch.put('foo', 'bar')
  batch.delete('bar')
end

# read from a snapshot
db.put('foo', 'bar')
snapshot = db.snapshot
db.put('foo', 'baz')
puts snapshot.get('foo') # => 'bar'
puts db.get('foo') # => 'baz'
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## License

http://opensource.org/licenses/BSD-3-Clause