# encoding: utf-8

require 'spec_helper'


describe LevelDb do
  around do |example|
    Dir.mktmpdir do |path|
      Dir.chdir(path, &example)
    end
  end

  let :db_path do
    File.expand_path('hello_world')
  end

  describe '.open' do
    it 'creates a database' do
      LevelDb.open(db_path)
      Dir.entries('.').should include('hello_world')
    end

    it 'opens an existing database' do
      db = LevelDb.open(db_path)
      db.close
      LevelDb.open(db_path)
    end

    context 'when disabling the create_if_missing option' do
      it 'complains if the database doesn\'t exist' do
        expect { LevelDb.open(db_path, create_if_missing: false) }.to raise_error(LevelDb::Error)
      end
    end

    context 'when enabling the error_if_exists option' do
      it 'complains if the database exists' do
        db = LevelDb.open(db_path)
        db.close
        expect { LevelDb.open(db_path, error_if_exists: true) }.to raise_error(LevelDb::Error)
      end
    end
  end

  describe '.repair' do
    it 'repairs the DB at a path' do
      LevelDb.open(db_path).close
      LevelDb.repair(db_path)
    end
  end

  describe '.destroy' do
    it 'destroys the DB at a path' do
      LevelDb.open(db_path).close
      LevelDb.destroy(db_path)
      Dir.entries('.').should_not include('hello_world')
    end
  end

  describe LevelDb::Db do
    let :db do
      LevelDb.open(db_path)
    end

    after do
      db.close
    end

    describe '#compact_range' do
      let :leveldb do
        leveldb = double(:db)
        leveldb.stub(:compact_range) do |*args|
          calls << args.map { |arg| arg && String.from_java_bytes(arg) }
        end
        leveldb
      end

      let :calls do
        []
      end

      it 'compacts the given range' do
        LevelDb::Db.new(leveldb).compact_range(from: 'one', to: 'three')
        calls.should include(['one', 'three'])
      end

      it "compacts the whole range to the 'to' key when the 'from' option is omitted" do
        LevelDb::Db.new(leveldb).compact_range(to: 'three')
        calls.should include([nil, 'three'])
      end

      it "compacts the whole range from the 'from' key when the 'to' option is omitted" do
        LevelDb::Db.new(leveldb).compact_range(from: 'one')
        calls.should include(['one', nil])
      end

      it "compacts the whole range when the both the 'from' and 'to' options are omitted" do
        LevelDb::Db.new(leveldb).compact_range
        calls.should include([nil, nil])
      end
    end

    describe '#close' do
      it 'closes the database' do
        db = double(:db)
        db.stub(:close)
        LevelDb::Db.new(db).close
        db.should have_received(:close)
      end
    end

    describe '#put/#get/#delete' do
      it 'puts a value and reads it back' do
        db.put('some', 'value')
        db.get('some').should == 'value'
      end

      it 'returns nil if no value is found' do
        db.get('hello').should be_nil
      end

      it 'complains when the value is nil' do
        expect { db.put('hello', nil) }.to raise_error(ArgumentError)
      end

      it 'complains when the key is nil' do
        expect { db.put(nil, 'hello') }.to raise_error(ArgumentError)
        expect { db.get(nil) }.to raise_error(ArgumentError)
        expect { db.delete(nil) }.to raise_error(ArgumentError)
      end

      it 'deletes a value' do
        db.put('some', 'value')
        db.delete('some')
        db.get('some').should be_nil
      end

      it 'doesn\'t complain when deleting things that don\'t exist' do
        expect { db.delete('some') }.to_not raise_error
      end
    end

    describe '#batch' do
      it 'does multiple operations in one go' do
        db.put('some', 'value')
        db.batch do |batch|
          batch.delete('some')
          batch.put('another', 'value')
          batch.put('more', 'data')
        end
        db.get('some').should be_nil
        db.get('another').should == 'value'
        db.get('more').should == 'data'
      end
    end

    describe '#snapshot' do
      it 'creates a view of the database at a specific point in time' do
        db.put('one', '1')
        snapshot = db.snapshot
        db.put('one', '3')
        snapshot.get('one').should == '1'
        db.get('one').should == '3'
        snapshot.close
      end
    end

    describe '#each' do
      before do
        db.put('one', '1')
        db.put('two', '2')
        db.put('three', '3')
        db.put('four', '4')
        db.put('five', '5')
      end

      it 'scans through the database' do
        seen = []
        db.each do |key, value|
          seen << [key, value]
        end
        seen.transpose.should == [%w[five four one three two], %w[5 4 1 3 2]]
      end

      it 'does nothing with an empty database' do
        called = false
        empty_db = LevelDb.open("#{db_path}_empty")
        empty_db.each { |k, v| called = true }
        called.should be_false
        empty_db.close
      end

      context 'with and offset, range and/or limit' do
        it 'scans from the offset to the end of the database' do
          seen = []
          db.each(from: 'one') do |key, _|
            seen << key
          end
          seen.should == %w[one three two]
        end

        it 'returns a Enumerable with the same behaviour' do
          seen = []
          enum = db.each(from: 'three')
          enum.to_a.should == [['three', '3'], ['two', '2']]
        end

        it 'scans up to a key' do
          seen = db.each(to: 'three').to_a.map(&:first)
          seen.should == %w[five four one three]
        end

        it 'scans up to a specified number of values' do
          seen = db.each(limit: 3).to_a.map(&:first)
          seen.should == %w[five four one]
        end

        it 'scans everything if the limit is larger than the database' do
          seen = db.each(limit: 100).to_a.map(&:first)
          seen.should == %w[five four one three two]
        end

        it 'combines the offset, range and limit options' do
          seen = db.each(from: 'four', to: 'three', limit: 2).to_a.map(&:first)
          seen.should == %w[four one]
          seen = db.each(from: 'four', to: 'three', limit: 4).to_a.map(&:first)
          seen.should == %w[four one three]
        end

        it 'does not require the offset key to exist' do
          seen = db.each(from: 'f').to_a.map(&:first)
          seen.first.should == 'five'
        end

        it 'does not require the end key to exist' do
          seen = db.each(to: 'o').to_a.map(&:first)
          seen.last.should == 'four'
        end
      end

      context 'when scanning in reverse' do
        it 'scans from the end of the database to the beginning' do
          result = db.each(reverse: true).to_a.map(&:first)
          result.should == %w[two three one four five]
        end

        it 'works with ranges' do
          result = db.each(from: 'three', to: 'four', reverse: true).to_a.map(&:first)
          result.should == %w[three one four]
          result = db.each(from: 'three', limit: 2, reverse: true).to_a.map(&:first)
          result.should == %w[three one]
        end

        it 'starts with the right element' do
          result = db.each(from: 'three', reverse: true).to_a.map(&:first)
          result.first.should == 'three'
          result = db.each(from: "three\xff", reverse: true).to_a.map(&:first)
          result.first.should == 'three'
        end

        it 'works when starting beyond the last element' do
          result = db.each(from: 'x', reverse: true).to_a.map(&:first)
          result.should == %w[two three one four five]
        end

        it 'works when starting before the first element' do
          result = db.each(from: 'a', reverse: true).to_a.map(&:first)
          result.should == %w[]
        end
      end

      context 'when using the returned Enumerable' do
        it 'supports external enumeration' do
          enum = db.each(from: 'three', limit: 2)
          enum.next.should == ['three', '3']
          enum.next.should == ['two', '2']
          expect { enum.next }.to raise_error(StopIteration)
        end

        it 'supports #next? to avoid raising StopIteration' do
          enum = db.each(from: 'three', limit: 2)
          enum.next
          enum.next?.should be_true
          enum.next
          enum.next?.should be_false
        end

        it 'is rewindable' do
          enum = db.each(from: 'three', limit: 2)
          enum.next.should == ['three', '3']
          enum.next.should == ['two', '2']
          enum.rewind
          enum.next.should == ['three', '3']
          enum.next.should == ['two', '2']
        end

        it 'supports lazy #map' do
          called = false
          enum = db.each(from: 'three', limit: 2)
          transformed = enum.map { |k, v| called = true; v }.map { |v| v.to_i * 2 }
          called.should be_false
          transformed.each { }
          called.should be_true
        end

        it 'supports lazy #select' do
          called = false
          enum = db.each(from: 'three', limit: 2)
          filtered = enum.select { |k, v| called = true; v == '3' }.select { |k, v| true }
          called.should be_false
          filtered.each { }
          called.should be_true
        end
      end
    end
  end
end