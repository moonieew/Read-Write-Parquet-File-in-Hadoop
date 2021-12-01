  private static void readParquetFile() {
    ParquetReader reader = null;
    Path path =	new	Path("/user/out/data.parquet");
    try {
      reader = AvroParquetReader
                .builder(path)
                .withConf(new Configuration())
                .build();
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        System.out.println(record);
      }
    }catch(IOException e) {
      e.printStackTrace();
    }finally {
      if(reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }