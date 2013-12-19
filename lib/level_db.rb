# encoding: utf-8

require 'leveldbjni-all-1.8'


module LevelDb
  module Library
    include_package 'org.iq80.leveldb'
    include_package 'org.fusesource.leveldbjni'

    module Internal
      include_package 'org.fusesource.leveldbjni.internal'
    end
  end

  def self.open(path, options={})
    Db.new(Library::JniDBFactory.factory.open(java.io.File.new(path), create_db_options(options)))
  rescue Library::Internal::NativeDB::DBException => e
    raise LevelDbError, e.message, e.backtrace
  end

  def self.repair(path, options={})
    Library::JniDBFactory.factory.repair(java.io.File.new(path), create_db_options(options))
    true
  end

  def self.destroy(path, options={})
    Library::JniDBFactory.factory.destroy(java.io.File.new(path), create_db_options(options))
    true
  end

  def self.create_db_options(options)
    options.each_with_object(Library::Options.new) do |(key, value), db_options|
      if OPEN_OPTIONS.include?(key)
        method_name, java_type = OPEN_OPTIONS[key]
        db_options.java_send(method_name, [java_type], value)
      end
    end
  end

  LevelDbError = Class.new(StandardError)

  module Encoding
    def encode_key(str)
      str.to_java_bytes
    end

    def encode_value(str)
      str.to_java_bytes
    end

    def decode_key(str)
      String.from_java_bytes(str)
    end

    def decode_value(str)
      String.from_java_bytes(str)
    end
  end

  module Crud
    include Encoding

    def get(key)
      value = @db.get(encode_key(key))
      value && decode_value(value)
    end

    def put(key, value)
      @db.put(encode_key(key), encode_value(value))
    end

    def delete(key)
      @db.delete(encode_key(key))
    end
  end

  class Db
    include Crud

    def initialize(db)
      @db = db
    end

    def close
      @db.close
    end

    def batch(&block)
      batch = @db.create_write_batch
      begin
        yield Batch.new(batch)
        @db.write(batch)
      ensure
        batch.close
      end
    end

    def each(options={}, &block)
      iterator = @db.iterator
      if options && (offset = options[:from])
        iterator.seek(encode_key(offset))
      elsif options[:reverse]
        iterator.seek_to_last
      else
        iterator.seek_to_first
      end
      scan = Scan.new(iterator, options[:reverse])
      scan.each(&block) if block_given?
      scan
    end

    def snapshot
      Snapshot.new(@db)
    end
  end

  class Scan
    include Encoding
    include Enumerable

    def initialize(iterator, reverse=false)
      @iterator = iterator
      @reverse = reverse
    end

    def each
      return self unless block_given?
      if @reverse
        entry = @iterator.peek_next
        while entry
          yield decode_key(entry.key), decode_value(entry.value)
          entry = @iterator.has_prev && @iterator.prev
        end
      else
        while @iterator.has_next
          entry = @iterator.next
          yield decode_key(entry.key), decode_value(entry.value)
        end
      end
    end
  end

  class Snapshot
    include Encoding

    def initialize(db)
      @db = db
      @snapshot = @db.snapshot
      @read_options = Library::ReadOptions.new
      @read_options.snapshot(@snapshot)
    end

    def get(key)
      value = @db.get(encode_key(key), @read_options)
      value && decode_value(value)
    end

    def close
      @snapshot.close
    end
  end

  class Batch
    include Crud

    def initialize(batch)
      @db = batch
    end
  end

  private

  OPEN_OPTIONS = {
    :create_if_missing => [:createIfMissing, Java::boolean],
    :error_if_exists => [:errorIfExists, Java::boolean],
    :paranoid_checks =>  [:paranoidChecks, Java::boolean],
    :write_buffer_size => [:writeBufferSize, Java::int],
    :max_open_files => [:maxOpenFiles, Java::int],
    :block_restart_interval => [:blockRestartInterval, Java::int],
    :block_size => [:blockSize, Java::int],
  }.freeze
end