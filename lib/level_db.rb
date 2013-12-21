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
    raise self::Error, e.message, e.backtrace
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

  Error = Class.new(StandardError)

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
      scan = Scan.new(@db.iterator, options)
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

    def initialize(iterator, options={})
      @iterator = iterator
      @from = options[:from]
      @to = options[:to]
      @reverse = options[:reverse]
      @limit = options[:limit]
    end

    def next
      raise StopIteration unless next?
      v, @next = @next, nil
      v
    end

    def next?
      init
      @next = internal_next unless @next
      !!@next
    end

    def each
      return self unless block_given?
      yield self.next while next?
    end

    private

    def init
      return if @started
      @started = true
      @count = 0
      if @from
        @iterator.seek(encode_key(@from))
      elsif @reverse
        @iterator.seek_to_last
      else
        @iterator.seek_to_first
      end
    end

    def internal_next
      if @reverse
        reverse_next
      else
        forward_next
      end
    end

    def reverse_next
      return nil if @limit && @count >= @limit
      return nil if @exhausted
      entry = @iterator.peek_next
      if entry
        key = decode_key(entry.key)
        return nil if @to && key < @to
        @count += 1
        @exhausted = !@iterator.has_prev
        @exhausted || @iterator.prev
        return key, decode_value(entry.value)
      end
    end

    def forward_next
      return nil if @limit && @count >= @limit
      return nil if @exhausted
      entry = @iterator.has_next && @iterator.peek_next
      if entry
        key = decode_key(entry.key)
        return nil if @to && key > @to
        @count += 1
        @iterator.has_next && @iterator.next
        return key, decode_value(entry.value)
      else
        @exhausted = true
        nil
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