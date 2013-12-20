# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'level_db/version'

Gem::Specification.new do |spec|
  spec.name          = 'leveldb-jruby'
  spec.platform      = 'java'
  spec.version       = LevelDb::VERSION
  spec.authors       = ['Theo Hultberg']
  spec.email         = %w[theo@iconara.net]
  spec.description   = %q{LevelDB for JRuby over JNI}
  spec.summary       = %q{LevelDB for JRuby}
  spec.homepage      = 'http://github.com/iconara/leveldb-jruby'
  spec.license       = 'BSD 3-Clause License'

  spec.files         = Dir['lib/**/*.rb', 'bin/*', 'README.md']
  spec.require_paths = %w(lib)
end
