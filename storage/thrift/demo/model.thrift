struct Record {
    1: string a,
    2: string b,
}

service ImageService {
   void ping(),

   i32 openRecords(),
   Record getRecord(1:i32 streamID),
   void close(),
}
