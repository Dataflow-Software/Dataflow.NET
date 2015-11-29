using System;
using Dataflow.Core;
using Dataflow.Remoting;

namespace tutorial {
	
	public sealed partial class Person : Message {
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static readonly MessageDescriptor Descriptor = new MessageDescriptor_20( "Person", Pbs.iNone,
			new Person(),
			new FieldDescriptor( "name", 10, 133 ),
			new FieldDescriptor( "id", 16, 129 ),
			new FieldDescriptor( "email", 26, 5 ),
			new FieldDescriptor( "phone", 34, 79, PhoneNumber.Descriptor )
		);
		public enum PhoneType {
			MOBILE = 0, HOME = 1, WORK = 2
		}
		
		public class PhoneTypeEnumInfo : EnumDescriptor {
			public static EnumDescriptor Descriptor = new EnumDescriptor( "PhoneType",
				new EnumFieldDescriptor( "MOBILE", 0 ),
				new EnumFieldDescriptor( "HOME", 1 ),
				new EnumFieldDescriptor( "WORK", 2 )
			);
		}
		public sealed partial class PhoneNumber : Message {
			public override MessageDescriptor GetDescriptor() { return Descriptor; }
			public static readonly MessageDescriptor Descriptor = new MessageDescriptor_20( "PhoneNumber", Pbs.iNone,
				new PhoneNumber(),
				new FieldDescriptor( "number", 10, 133 ),
				new FieldDescriptor( "type", 16, 16, PhoneTypeEnumInfo.Descriptor )
			);
			// fields bit-masks.
			private int _b0;
			// required string number; id = 1;
			public string number { get; set; }
			// optional PhoneType type; id = 2;
			public PhoneType type { get { return _type; } set { _b0|=1; _type= value; } }
			#region message fields
			public void ClearNumber() { number = null; }
			public bool HasNumber { get { return number != null; } }
			private PhoneType _type;
			public void ClearType() { _b0&=~1; _type = PhoneType.HOME; }
			public bool HasType { get { return (_b0&1) != 0; } }
			#endregion
			#region message methods
			public PhoneNumber() {
				_type = PhoneType.HOME;
			}
			public override Message New() { return new PhoneNumber(); }
			public override void Clear() {
				_memoized_size = _b0 = 0;
				number = null;
				_type = PhoneType.HOME;
			}
			public override bool Equals( Message msg ) {
				var test = msg as PhoneNumber;
				if(test==null) return false;
				if(number != test.number) return false;
				if(type != test.type) return false;
				return true;
			}
			public override bool IsInitialized() {
				if( !HasNumber ) return false;
				return true;
			}
			public override void Get( FieldDescriptor ds_, IDataReader dr_ ) {
				switch( ds_.Pos ) {
					case 0: number = dr_.AsString(); break;
					case 1: type = (PhoneType)dr_.AsEnum(PhoneTypeEnumInfo.Descriptor); break;
				}
			}
			public override int GetSerializedSize() {
				var size = 0;
				if( HasNumber ) size += 1 + Pbs.str(number);
				if( HasType ) size += 2;
				return _memoized_size = size;
			}
			public override void Put( IDataWriter wr_ ) {
				var fs_ = Descriptor.Fields;
				if( HasNumber ) wr_.AsString( fs_[0], number );
				if( HasType ) wr_.AsEnum( fs_[1], (int)_type );
			}
			#endregion
		}
		// fields bit-masks.
		private int _b0;
		// required string name; id = 1;
		public string name { get; set; }
		// required int32 id; id = 2;
		public int id { get { return _id; } set { _b0|=1; _id= value; } }
		// optional string email; id = 3;
		public string email { get { return _email; } set { _b0|=2; _email= value; } }
		// repeated PhoneNumber phone; id = 4;
		public PhoneNumber[] phone { get { return _phone.Items; } set { _phone.Items = value; } }
		#region message fields
		public void ClearName() { name = null; }
		public bool HasName { get { return name != null; } }
		private int _id;
		public void ClearId() { _b0&=~1; _id = 0; }
		public bool HasId { get { return (_b0&1) != 0; } }
		private string _email;
		public void ClearEmail() { _b0&=~2; _email = "lamer@aol.com"; }
		public bool HasEmail { get { return (_b0&2) != 0; } }
		private Repeated<PhoneNumber> _phone;
		public void ClearPhone() { _phone.Clear(); }
		public int PhoneCount { get { return _phone.Count; } }
		public PhoneNumber AddPhone( PhoneNumber i ) { return _phone.Add(i); }
		public PhoneNumber GetPhone( int i ) { return _phone[i]; }
		#endregion
		#region message methods
		public Person() {
			_email = "lamer@aol.com";
		}
		public override Message New() { return new Person(); }
		public override void Clear() {
			_memoized_size = _b0 = 0;
			name = null;
			_id = 0;
			_email = "lamer@aol.com";
			_phone.Clear();
		}
		public override bool Equals( Message msg ) {
			var test = msg as Person;
			if(test==null) return false;
			if(name != test.name) return false;
			if(id != test.id) return false;
			if(email != test.email) return false;
			if(_phone.Count != test.PhoneCount) return false;
			for( var i = 0; i < _phone.Count; i++ ) if((phone[i]==null&&test.phone[i]!=null) || !phone[i].Equals(test.phone[i])) return false;
			return true;
		}
		public override bool IsInitialized() {
			if( !HasName ) return false;
			if( !HasId ) return false;
			for(var i=0; i<_phone.Count; i++) if(!_phone[i].IsInitialized()) return false;
			return true;
		}
		public override void Get( FieldDescriptor ds_, IDataReader dr_ ) {
			switch( ds_.Pos ) {
				case 0: name = dr_.AsString(); break;
				case 1: id = dr_.AsInt(); break;
				case 2: email = dr_.AsString(); break;
				case 3: { var tms3_= new PhoneNumber(); dr_.AsMessage(tms3_); _phone.Add(tms3_); } break;
			}
		}
		public override int GetSerializedSize() {
			var size = 0;
			if( HasName ) size += 1 + Pbs.str(name);
			if( HasId ) size += 1 + Pbs.i32(_id);
			if( HasEmail ) size += 1 + Pbs.str(_email);
			var count_ = _phone.Count;
			if( count_ > 0 ) { size += count_; foreach(var item in _phone.Items) size += Pbs.msg(item); }
			return _memoized_size = size;
		}
		public override void Put( IDataWriter wr_ ) {
			var fs_ = Descriptor.Fields;
			if( HasName ) wr_.AsString( fs_[0], name );
			if( HasId ) wr_.AsInt( fs_[1], _id );
			if( HasEmail ) wr_.AsString( fs_[2], _email );
			if( _phone.Count > 0 ) wr_.AsRepeated( fs_[3], _phone.Items);
		}
		#endregion
	}
	
	public sealed partial class AddressBook : ListOfMessage<Person> {
		public override MessageDescriptor GetDescriptor() { return Descriptor; }
		public static readonly MessageDescriptor Descriptor = new MessageDescriptor_20( "AddressBook", Pbs.iList,
			new AddressBook(),
			new FieldDescriptor( "persona", 10, 79, Person.Descriptor )
		);
		// repeated Person persona; id = 1;
		public Person[] persona { get { return _msgs.Items; } set { _msgs.Items = value; } }
		#region message fields
		public void ClearPersona() { _msgs.Clear(); }
		public int PersonaCount { get { return _msgs.Count; } }
		public Person AddPersona( Person i ) { return _msgs.Add(i); }
		public Person GetPersona( int i ) { return _msgs[i]; }
		#endregion
		#region message methods
		public AddressBook() {}
		public AddressBook( Person[] value ) { persona = value; }
		public override Message New() { return new AddressBook(); }
		#endregion
	}
}