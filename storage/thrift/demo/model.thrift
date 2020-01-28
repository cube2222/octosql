struct Foo {
    1: string fooString,
    2: i16 fooInt16,
    3: i32 fooInt32,
    4: i64 fooInt64,
    5: double fooDouble,
    6: byte fooByte,
    7: bool fooBool,
}

struct Bar {
    1: string barString,
}

struct Record {
    1: string a,
    2: string b,
    3: optional Bar bar,
    4: required Foo foo,
    5: list<string> lst,
}

service ImageService {
   void ping(),

   i32 openRecords(),
   Record getRecord(1:i32 streamID),
   void close(),
}
