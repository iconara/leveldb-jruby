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
        expect { LevelDb.open(db_path, create_if_missing: false) }.to raise_error(LevelDb::LevelDbError)
      end
    end

    context 'when enabling the error_if_exists option' do
      it 'complains if the database exists' do
        db = LevelDb.open(db_path)
        db.close
        expect { LevelDb.open(db_path, error_if_exists: true) }.to raise_error(LevelDb::LevelDbError)
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

      context 'with an offset' do
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
      end

      context 'with reverse: true' do
        it 'scans from the end of the database to the beginning' do
          db.each(reverse: true).to_a.map(&:first).should == %w[two three one four five]
        end
      end
    end
  end
end