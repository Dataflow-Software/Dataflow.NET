// autogenerated by Dataflow Software implementation of Protocol Buffers to C# compiler

using System;

namespace Dataflow.Serialization
{
	// package: Dataflow.Serialization
	
	public sealed partial class JsonValueTest : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("JsonValueTest");
			_init_ds_(_boot_ds_, new JsonValueTest(),
				new FieldDescriptor("name", 10, 5),
				new FieldDescriptor("data", 18, 15, Struct.Descriptor)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		
		// optional string name = 1;
		public string Name { get { return _name; } set { _name = value; } }
		// optional Struct data = 2;
		public Struct Data { get { return _data; } set { _data = value; } }
		
		#region message fields
		
		private string _name;
		public void ClearName() { _name = null; }
		public bool HasName { get { return _name != null; } }
		private Struct _data;
		public void ClearData() { _data = null; }
		public bool HasData { get { return _data != null; } }
		
		#endregion
		
		#region message methods
		
		public override Message New() { return new JsonValueTest(); }
		public override void Clear()
		{
			_memoized_size = 0;
			_name = null;
			_data = null;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as JsonValueTest;
			if(test==null) return false;
			if(_name != test._name) return false;
			if(!_data.Equals(test._data)) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			switch( ds_.Pos )
			{
				case 0: Name = dr_.AsString(); break;
				case 1: dr_.AsMessage(_data??(Data = new Struct()), ds_); break;
			}
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasName) _gss_+= 1 + Pbs.str(_name);
			if(HasData) _gss_+= 1 + Pbs.msg(_data);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasName) wr_.AsString(fs_[0], _name);
			if(HasData) wr_.AsMessage(fs_[1], _data);
		}
		
		#endregion
	}
	
	public sealed partial class BoolValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("BoolValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new BoolValue(),
				new FieldDescriptor("Value", 8, 2061)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional bool Value = 1;
		public bool Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private bool _value;
		public void ClearValue() { _b0&=~0x01; _value = false; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public BoolValue() {}
		public BoolValue( bool value ) { Value = value; }
		public override Message New() { return new BoolValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = false;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as BoolValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsBool();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 2;
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsBool(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class BytesValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("BytesValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new BytesValue(),
				new FieldDescriptor("Value", 10, 2054)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		
		// optional bytes Value = 1;
		public byte[] Value { get { return _value; } set { _value = value; } }
		
		#region message fields
		
		private byte[] _value;
		public void ClearValue() { _value = null; }
		public bool HasValue { get { return _value != null; } }
		
		#endregion
		
		#region message methods
		
		public BytesValue() {}
		public BytesValue( byte[] value ) { Value = value; }
		public override Message New() { return new BytesValue(); }
		public override void Clear()
		{
			_memoized_size = 0;
			_value = null;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as BytesValue;
			if(test==null) return false;
			if(!Pbs.EqualBytes(_value,test._value)) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsBytes();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.bts(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsBytes(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class CharValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("CharValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new CharValue(),
				new FieldDescriptor("Value", 8, 2062)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional char Value = 1;
		public char Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private char _value;
		public void ClearValue() { _b0&=~0x01; _value = '\0'; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public CharValue() {}
		public CharValue( char value ) { Value = value; }
		public override Message New() { return new CharValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = '\0';
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as CharValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsChar();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.chr(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsChar(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class Int32Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("Int32Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new Int32Value(),
				new FieldDescriptor("Value", 8, 2049)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional int32 Value = 1;
		public int Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private int _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public Int32Value() {}
		public Int32Value( int value ) { Value = value; }
		public override Message New() { return new Int32Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as Int32Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsInt();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.i32(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsInt(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class SInt32Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("SInt32Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new SInt32Value(),
				new FieldDescriptor("Value", 8, 3073)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional sint32 Value = 1;
		public int Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private int _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public SInt32Value() {}
		public SInt32Value( int value ) { Value = value; }
		public override Message New() { return new SInt32Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as SInt32Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsSi32();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.si32(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsSi32(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class UInt32Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("UInt32Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new UInt32Value(),
				new FieldDescriptor("Value", 8, 2049)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional uint32 Value = 1;
		public uint Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private uint _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public UInt32Value() {}
		public UInt32Value( uint value ) { Value = value; }
		public override Message New() { return new UInt32Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as UInt32Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = (uint)dr_.AsInt();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.i32((int)_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsInt(fs_[0], (int)_value);
		}
		
		#endregion
	}
	
	public sealed partial class Int64Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("Int64Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new Int64Value(),
				new FieldDescriptor("Value", 8, 2050)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional int64 Value = 1;
		public long Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private long _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public Int64Value() {}
		public Int64Value( long value ) { Value = value; }
		public override Message New() { return new Int64Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as Int64Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsLong();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.i64(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsLong(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class SInt64Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("SInt64Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new SInt64Value(),
				new FieldDescriptor("Value", 8, 3074)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional sint64 Value = 1;
		public long Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private long _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public SInt64Value() {}
		public SInt64Value( long value ) { Value = value; }
		public override Message New() { return new SInt64Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as SInt64Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsSi64();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.si64(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsSi64(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class UInt64Value : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("UInt64Value", Pbs.iBox);
			_init_ds_(_boot_ds_, new UInt64Value(),
				new FieldDescriptor("Value", 8, 2050)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional uint64 Value = 1;
		public ulong Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private ulong _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public UInt64Value() {}
		public UInt64Value( ulong value ) { Value = value; }
		public override Message New() { return new UInt64Value(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as UInt64Value;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = (ulong)dr_.AsLong();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.i64((long)_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsLong(fs_[0], (long)_value);
		}
		
		#endregion
	}
	
	public sealed partial class DoubleValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("DoubleValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new DoubleValue(),
				new FieldDescriptor("Value", 9, 2060)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional double Value = 1;
		public double Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private double _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public DoubleValue() {}
		public DoubleValue( double value ) { Value = value; }
		public override Message New() { return new DoubleValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as DoubleValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsDouble();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 9;
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsDouble(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class FloatValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("FloatValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new FloatValue(),
				new FieldDescriptor("Value", 13, 2059)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional float Value = 1;
		public float Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private float _value;
		public void ClearValue() { _b0&=~0x01; _value = 0; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public FloatValue() {}
		public FloatValue( float value ) { Value = value; }
		public override Message New() { return new FloatValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = 0;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as FloatValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsFloat();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 5;
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsFloat(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class StringValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("StringValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new StringValue(),
				new FieldDescriptor("Value", 10, 2053)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		
		// optional string Value = 1;
		public string Value { get { return _value; } set { _value = value; } }
		
		#region message fields
		
		private string _value;
		public void ClearValue() { _value = null; }
		public bool HasValue { get { return _value != null; } }
		
		#endregion
		
		#region message methods
		
		public StringValue() {}
		public StringValue( string value ) { Value = value; }
		public override Message New() { return new StringValue(); }
		public override void Clear()
		{
			_memoized_size = 0;
			_value = null;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as StringValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsString();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.str(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsString(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class DateTimeValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("DateTimeValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new DateTimeValue(),
				new FieldDescriptor("Value", 8, 2055)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional datetime Value = 1;
		public DateTime Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private DateTime _value;
		public void ClearValue() { _b0&=~0x01; _value = DateTime.MinValue; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public DateTimeValue() {}
		public DateTimeValue( DateTime value ) { Value = value; }
		public override Message New() { return new DateTimeValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = DateTime.MinValue;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as DateTimeValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsDate();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.dat(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsDate(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class CurrencyValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("CurrencyValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new CurrencyValue(),
				new FieldDescriptor("Value", 8, 2065)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional currency Value = 1;
		public Currency Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private Currency _value;
		public void ClearValue() { _b0&=~0x01; _value = Currency.Zero; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public CurrencyValue() {}
		public CurrencyValue( Currency value ) { Value = value; }
		public override Message New() { return new CurrencyValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = Currency.Zero;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as CurrencyValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsCurrency();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.cur(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsCurrency(fs_[0], _value);
		}
		
		#endregion
	}
	
	public sealed partial class DecimalValue : Message
	{
		private static MessageDescriptor _boot_ds_; 
		private static MessageDescriptor _boot_ds() 
		{
			_boot_ds_= new MessageDescriptor_30("DecimalValue", Pbs.iBox);
			_init_ds_(_boot_ds_, new DecimalValue(),
				new FieldDescriptor("Value", 10, 2056)
			);
			return _boot_ds_;
		}
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static MessageDescriptor Descriptor { get { return _boot_ds_??_boot_ds(); } }
		// bit-masks for tracking nulls in value type fields.
		private int _b0;
		
		// optional decimal Value = 1;
		public decimal Value { get { return _value; } set { _b0|=0x01; _value = value; } }
		
		#region message fields
		
		private decimal _value;
		public void ClearValue() { _b0&=~0x01; _value = Decimal.Zero; }
		public bool HasValue { get { return (_b0&0x01) != 0; } }
		
		#endregion
		
		#region message methods
		
		public DecimalValue() {}
		public DecimalValue( decimal value ) { Value = value; }
		public override Message New() { return new DecimalValue(); }
		public override void Clear()
		{
			_memoized_size = _b0 = 0;
			_value = Decimal.Zero;
		}
		public override int GetHashCode() { return base.GetHashCode(); }
		public override bool Equals(object msg)
		{
			var test = msg as DecimalValue;
			if(test==null) return false;
			if(_value != test._value) return false;
			return true;
		}
		public override void Get(FieldDescriptor ds_, IDataReader dr_)
		{
			Value = dr_.AsDecimal();
		}
		public override int GetSerializedSize()
		{
			int _gss_= 0;
			if(HasValue) _gss_+= 1 + Pbs.dec(_value);
			return _memoized_size = _gss_;
		}
		public override void Put(IDataWriter wr_)
		{
			var fs_ = Descriptor.Fields;
			if(HasValue) wr_.AsDecimal(fs_[0], _value);
		}
		
		#endregion
	}
}