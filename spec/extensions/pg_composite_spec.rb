require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
  Sequel.extension :pg_composite
rescue LoadError => e
  skip_warn "can't load pg_composite extension (#{e.class}: #{e})"
else
  describe "pg_composite extension" do
    before do
      @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
      @m = Sequel::Postgres::PGComposite
      @p = @m::Parser
    end
    
    
    it "should parse single attribute" do
      # text
      @p.parse('(a)', [[:col1, :text]]).should == {:col1 => 'a'}
      @p.parse('("a b")', [[:col1, :text]]).should == {:col1 => 'a b'}
      @p.parse('("a "" b")', [[:col1, :text]]).should == {:col1 => 'a " b'}
      @p.parse('("")', [[:col1, :text]]).should == {:col1 => ''}
      
      # integer
      @p.parse('(0)', [[:col1, :integer]]).should == {:col1 => 0}
      @p.parse('(1)', [[:col1, :integer]]).should == {:col1 => 1}
      @p.parse('(-1)', [[:col1, :integer]]).should == {:col1 => -1}

      # float
      @p.parse('(0.0)', [[:col1, :float]]).should == {:col1 => 0.0}
      @p.parse('(1.1)', [[:col1, :float]]).should == {:col1 => 1.1}
      @p.parse('(-1.1)', [[:col1, :float]]).should == {:col1 => -1.1}
      
      # date
      @p.parse('(2012-01-01)', [[:col1, :date]]).should == {:col1 => Sequel.string_to_date('2012-01-01')}

      # time
      @p.parse('(00:00:00)', [[:col1, :time]]).should == {:col1 => Sequel.string_to_time('00:00:00')}
      
      # boolean
      @p.parse('(t)', [[:col1, :boolean]]).should == {:col1 => true}
      @p.parse('(f)', [[:col1, :boolean]]).should == {:col1 => false}
    end
    
    it "should parse NULL value" do
      @p.parse('(,a)', [[:col1, :text],[:col2, :text]]).should == {:col1 => nil, :col2 => 'a'}
      @p.parse('(a,)', [[:col1, :text],[:col2, :text]]).should == {:col1 => 'a', :col2 => nil}

      @p.parse('(,1)', [[:col1, :integer],[:col2, :integer]]).should == {:col1 => nil, :col2 => 1}
      @p.parse('(,-1)', [[:col1, :integer],[:col2, :integer]]).should == {:col1 => nil, :col2 => -1}
      @p.parse('(1,)', [[:col1, :integer],[:col2, :integer]]).should == {:col1 => 1, :col2 => nil}
      @p.parse('(-1,)', [[:col1, :integer],[:col2, :integer]]).should == {:col1 => -1, :col2 => nil}

      @p.parse('(,1.1)', [[:col1, :float],[:col2, :float]]).should == {:col1 => nil, :col2 => 1.1}
      @p.parse('(,-1.1)', [[:col1, :float],[:col2, :float]]).should == {:col1 => nil, :col2 => -1.1}
      @p.parse('(1.1,)', [[:col1, :float],[:col2, :float]]).should == {:col1 => 1.1, :col2 => nil}
      @p.parse('(-1.1,)', [[:col1, :float],[:col2, :float]]).should == {:col1 => -1.1, :col2 => nil}

      @p.parse('(,2012-01-01)', [[:col1, :date],[:col2, :date]]).should == {:col1 => nil, :col2 => Sequel.string_to_date('2012-01-01')}
      @p.parse('(2012-01-01,)', [[:col1, :date],[:col2, :date]]).should == {:col1 => Sequel.string_to_date('2012-01-01'), :col2 => nil}

      @p.parse('(,00:00:00)', [[:col1, :time],[:col2, :time]]).should == {:col1 => nil, :col2 => Sequel.string_to_time('00:00:00')}
      @p.parse('(00:00:00,)', [[:col1, :time],[:col2, :time]]).should == {:col1 => Sequel.string_to_time('00:00:00'), :col2 => nil}

      @p.parse('(,t)', [[:col1, :boolean],[:col2, :boolean]]).should == {:col1 => nil, :col2 => true}
      @p.parse('(t,)', [[:col1, :boolean],[:col2, :boolean]]).should == {:col1 => true, :col2 => nil}
      @p.parse('(,f)', [[:col1, :boolean],[:col2, :boolean]]).should == {:col1 => nil, :col2 => false}
      @p.parse('(f,)', [[:col1, :boolean],[:col2, :boolean]]).should == {:col1 => false, :col2 => nil}
    end

    it "should parse multiple values" do
      # text
      @p.parse('(aa,"xx yy")', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'aa', :col2 => 'xx yy'}
      @p.parse('(aa," xx yy")', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'aa', :col2 => ' xx yy'}
      @p.parse('(aa,"xx yy ")', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'aa', :col2 => 'xx yy '}
      @p.parse('(aa," xx yy ")', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'aa', :col2 => ' xx yy '}
      @p.parse('("xx yy",aa)', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'xx yy', :col2 => 'aa'}
      @p.parse('("xx yy ",aa)', [[:col1, :text], [:col2, :text]]).should == {:col1 => 'xx yy ', :col2 => 'aa'}
      @p.parse('(" xx yy",aa)', [[:col1, :text], [:col2, :text]]).should == {:col1 => ' xx yy', :col2 => 'aa'}
      @p.parse('(" xx yy ",aa)', [[:col1, :text], [:col2, :text]]).should == {:col1 => ' xx yy ', :col2 => 'aa'}
      @p.parse('(aa,,bb)', [[:col1, :text], [:col2, :text], [:col3, :text]]).should == {:col1 => 'aa', :col2 => nil, :col3 => 'bb'}

      # integer
      @p.parse('(1,0,-1)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => 1, :col2 => 0, :col3 => -1}
      @p.parse('(-1,0,1)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => -1, :col2 => 0, :col3 => 1}
      @p.parse('(,0,1)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => nil, :col2 => 0, :col3 => 1}
      @p.parse('(-1,,1)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => -1, :col2 => nil, :col3 => 1}
      @p.parse('(-1,,)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => -1, :col2 => nil, :col3 => nil}
      @p.parse('(,,)', [[:col1, :integer], [:col2, :integer], [:col3, :integer]]).should == {:col1 => nil, :col2 => nil, :col3 => nil}
    end


    it "should parse nested string values" do
      @p.parse('(aa,"(bb)")', [[:col1, :text],[:col2, :composite, [[:col2_1, :text]]]]).should == {:col1 => 'aa', :col2 => {:col2_1 => 'bb'}}
      @p.parse('(aa,"(bb)",cc)', [[:col1, :text],[:col2, :composite, [[:col2_1, :text]]],[:col3, :text]]).should == {:col1 => 'aa', :col2 => {:col2_1 => 'bb'}, :col3 => 'cc'}

      @p.parse('(aa,"(xx)","(yy)")', [
        [:col1, :text],
        [:col2, :composite, [
          [:col2_1, :text]
        ]],
        [:col3, :composite, [
          [:col3_1, :text]
        ]]
      ]).should == {:col1 => 'aa', :col2 => {:col2_1 => 'xx'}, :col3 => {:col3_1 => 'yy'}}

      @p.parse('("(""x x"")")', [
        [:col1, :composite, [
          [:col1_1, :text]
        ]]
      ]).should == {:col1 => {:col1_1 => 'x x'}}

      @p.parse('("(""(""""x """""""" x"""")"")")', [
        [:col1, :composite, [
          [:col2, :composite, [
            [:col2_1, :text]
          ]]
        ]]
      ]).should == {:col1 => {:col2 => {:col2_1 => 'x " x'}}}

      @p.parse('(aa,"(""xx"")")', [
        [:col1, :text],
        [:col2, :composite, [
          [:col2_1, :text]
        ]]
      ]).should == {:col1 => 'aa', :col2 => {:col2_1 => 'xx'}}

      @p.parse('(aa,"(""xx yy"")")', [[:col1, :text],[:col2, :composite, [[:col2_1, :text]]]]).should == {:col1 => 'aa', :col2 => {:col2_1 => 'xx yy'}}

      @p.parse('(aa,"(""(""""x """""""" x"""")"")",zz)', [
        [:col1, :text],
        [:col2, :composite, [
          [:col2_1, :composite, [
            [:col2_1_1, :text]
          ]]
        ]],
        [:col3, :text]
      ]).should == {:col1 => 'aa', :col2 => {:col2_1 => {:col2_1_1 => 'x " x'}}, :col3 => 'zz'}
      
    end
    
    it "should parse nested integer values" do
      @p.parse('(10,"(100)")', [[:col1, :integer],[:col2, :composite, [[:col2_1, :integer]]]]).should == {:col1 => 10, :col2 => {:col2_1 => 100}}

      @p.parse('(10,"(100)",20)', [
        [:col1, :integer],
        [:col2, :composite, [[:col2_1, :integer]]],
        [:col3, :integer]
      ]).should == {:col1 => 10, :col2 => {:col2_1 => 100}, :col3 => 20}

      @p.parse('(10,"(100,200)",20)', [
        [:col1, :integer],
        [:col2, :composite, [[:col2_1, :integer], [:col2_2, :integer]]],
        [:col3, :integer]
      ]).should == {:col1 => 10, :col2 => {:col2_1 => 100, :col2_2 => 200}, :col3 => 20}

      @p.parse('(10,"(100)",20,"(1000,""(5000)"")")', [
        [:col1, :integer],
        [:col2, :composite, [[:col2_1, :integer]]],
        [:col3, :integer],
        [:col4, :composite, [
          [:col4_1, :integer],
          [:col4_2, :composite, [
            [:col4_2_1, :integer]
          ]]
        ]]
      ]).should == {
        :col1 => 10,
        :col2 => {
          :col2_1 => 100
        },
        :col3 => 20,
        :col4 => {
          :col4_1 => 1000,
          :col4_2 => {
            :col4_2_1 => 5000
          }
        }
      }

    end
    
  end
end