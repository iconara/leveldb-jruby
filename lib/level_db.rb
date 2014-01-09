# encoding: utf-8

require 'leveldbjni-jars'


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
      return str unless str
      str.to_java_bytes
    end

    def encode_value(str)
      return str unless str
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
    rescue java.lang.IllegalArgumentException => e
      raise ArgumentError, e.message, e.backtrace
    end

    def put(key, value)
      @db.put(encode_key(key), encode_value(value))
    rescue java.lang.IllegalArgumentException => e
      raise ArgumentError, e.message, e.backtrace
    end

    def delete(key)
      @db.delete(encode_key(key))
    rescue java.lang.IllegalArgumentException => e
      raise ArgumentError, e.message, e.backtrace
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
      cursor = Cursor.new(@db.iterator, options)
      cursor.each(&block) if block_given?
      cursor
    end

    def snapshot
      Snapshot.new(@db)
    end
  end

  module LazyEnumerable
    include Enumerable

    def map(&transform)
      LazyMap.new(self, &transform)
    end

    def select(&filter)
      LazySelect.new(self, &filter)
    end
  end

  class LazyMap
    include LazyEnumerable

    def initialize(enum, &transform)
      @enum = enum
      @transform = transform
    end

    def each(&block)
      if block
        @enum.each do |element|
          block.call(@transform.call(element))
        end
      end
      self
    end
  end

  class LazySelect
    include LazyEnumerable

    def initialize(enum, &filter)
      @enum = enum
      @filter = filter
    end

    def each(&block)
      if block
        @enum.each do |element|
          block.call(element) if @filter.call(element)
        end
      end
      self
    end
  end

  class Cursor
    include Encoding
    include LazyEnumerable

    def initialize(iterator, options={})
      @iterator = iterator
      @from = options[:from]
      @to = options[:to]
      @reverse = options[:reverse]
      @limit = options[:limit]
      rewind
    end

    def close
      @iterator.close
    end

    def next
      raise StopIteration unless next?
      v, @next = @next, nil
      v
    end

    def next?
      @next = internal_next unless @next
      !!@next
    end

    def each
      return self unless block_given?
      rewind
      yield self.next while next?
      close
      self
    end

    def rewind
      @next = nil
      @started = false
      @exhausted = false
      @count = 0
    end

    private

    def init
      return if @started
      @started = true
      @count = 0
      if @from
        @iterator.seek(encode_key(@from))
        unless @iterator.has_next
          @iterator.seek_to_last
        end
      elsif @reverse
        @iterator.seek_to_last
      else
        @iterator.seek_to_first
      end
    end

    def internal_next
      init
      return nil if @exhausted || (@limit && @count >= @limit)
      if (entry = @iterator.has_next && @iterator.peek_next)
        key = decode_key(entry.key)
        if @reverse
          return nil if (@to && key < @to) || (@from && @from < key)
          @exhausted = !@iterator.has_prev
          @exhausted || @iterator.prev
        else
          return nil if (@to && key > @to) || (@from && @from > key)
          @exhausted = !@iterator.has_next
          @exhausted || @iterator.next
        end
        @count += 1
        return key, decode_value(entry.value)
      end
    rescue NativeException
      raise
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