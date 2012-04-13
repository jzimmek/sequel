module Sequel
  module Postgres
    PG_TYPES = {} unless defined?(PG_TYPES)
    
    module PGComposite

      def self.register!(oid, attributes)
        ::Sequel::Postgres::PG_TYPES[oid] = proc do |value|
          Parser.new(value).readComposite(attributes, 0)
        end
      end

      def self.auto_register!(db)
        sql = <<-SQL
          select 
            distinct
            a.udt_name,
            t.oid
          from 
            information_schema.attributes a
          join pg_catalog.pg_type t
          on
            t.typname = a.udt_name
        SQL

        db[sql].all.each do |type|
          register! type[:oid], auto_attributes(db, type)
        end

        db.reset_conversion_procs
      end

      private
      
      def self.auto_attributes(db, type)
        sql = <<-SQL
          select 
            attribute_name, 
            attribute_udt_name,
            data_type
          from 
            information_schema.attributes 
          where 
            udt_name = ? 
          order by 
            ordinal_position asc
        SQL
        
        attributes = db[sql, type[:udt_name]].map do |attr|
          if attr[:data_type] == "USER-DEFINED"
            [attr[:attribute_name].to_sym, :composite, auto_attributes(db, :udt_name => attr[:attribute_udt_name])]
          else
            [attr[:attribute_name].to_sym, attr[:attribute_udt_name].to_sym]
          end
        end
      end

      class Parser
        
        TYPES = {} unless defined?(TYPES)
        TYPES[:boolean] = proc do |p, level|
          res = p.chunk[0,1] == "t"
          p.pos += 1
          res
        end
        TYPES[:date] = proc do |p, level|
          # format of date is YYYY-MM-DD = 10 char
          res = Sequel.string_to_date p.chunk[0,10]
          p.pos += 10
          res
        end
        TYPES[:time] = proc do |p, level|
          # format of date is hh:mm:ss = 8 char
          res = Sequel.string_to_time p.chunk[0,8]
          p.pos += 8
          res
        end
        TYPES[:integer] = proc do |p, level|
          res = p.chunk.match(/([-]?[0-9]+)+/)
          p.pos += res[1].length
          res[1].to_i
        end
        TYPES[:float] = proc do |p, level|
          res = p.chunk.match(/([-]?[0-9]+\.[0-9]+)+/)
          p.pos += res[1].length
          res[1].to_f
        end
        TYPES[:text] = proc do |p, level|
          if p.chunk[0] != '"'
            pos = p.pos
            value = nil
            while !p.eof?
              if [',', ')'].include? p.readChar
                p.pos -= 1
                value = p.source[pos, p.pos-pos]
                break
              end
            end
            value
          else
            p.pos += 1 # skip the beginning " char
            pos = p.pos
            value = nil
            while !p.eof?
              if p.chunk[0,2] == '")' || p.chunk[0,2] == '",'
                value = p.source[pos, p.pos-pos]
                p.pos += 1
                break
              end
              p.readChar
            end

            # TODO: use more correct regex /^.../ and /...$/
            
            # puts "1--(#{level})---- '#{value}'" if p.debug
            
            value = value[((1 << level)-1), value.length - (((1 << level)-1)*2)] # remove quotes from begin/end

            # puts "2--(#{level})---- '#{value}'" if p.debug

            value = value.gsub('"'*(1 << level+1), '"') # unescape inline quotes 
            
            # puts "3--(#{level})---- '#{value}'" if p.debug
            
            value
            
            # value.gsub('"'*((1 << level)-1), '') # remove quotes from begin/end
          end
        end
        
        
        
        attr_accessor :pos, :source, :debug
        def initialize(source, opts={})
          @debug = (opts[:debug] == true)
          @source = source
          @pos = 0
        end

        def chunk
          @source[@pos, @source.length]
        end

        def readChar
          ch = @source[@pos]
          @pos += 1
          ch
        end

        def eof?
          !(@pos < @source.length)
        end

        def read(type, args, level)
          if chunk[0] == "," || chunk[0] == ")"
            return nil # null value
          else
            if type == :composite
              p = Parser.new chunk, :debug => @debug
              res = p.readComposite args, level + 1
              @pos += p.pos
              res
            elsif type == :array
              raise "array in composite is currently not implemented"
            else
              if p = TYPES[type]
                p.call self, level
              else
                raise "unsupported type: #{type}"
              end
            end
          end

        end

        def readComma
          ch = readChar
          raise "invalid comma: #{ch}" unless ch == ","
        end

        def readComposite(fields, level=0)
          res = {}
          raise "invalid composite start (level #{level}) : #{chunk}" unless chunk[0,level+1] == ('"'*level) + "("
          @pos += level+1
          fields.each_with_index do |field, idx|
            name, type, args = field
            value = read(type, args, level)
            
            # puts "name: #{name} -> '#{value}'" if @debug

            res[name.to_sym] = value

            readComma if idx < fields.length - 1
          end
          raise "invalid composite end (level #{level}) : #{chunk}" unless chunk[0,level+1] == (")" + '"'*level)
          @pos += level+1
          res
        end

        def self.parse(source, attributes, opts={})
          self.new(source, opts).readComposite attributes, 0
        end

      end


    end
  end
end

# $LOAD_PATH << "lib"
# require "rspec"
# 
# 
# puts Sequel::Postgres::PGComposite::Parser.parse('("(""(""""x x"""")"")")', [
#   [:col1, :composite, [
#     [:col1, :composite, [
#       [:col1_1, :text]
#     ]]
#   ]]
# ], :debug => true).inspect
# # .should == {:col1 => {:col1_1 => 'x x'}}
