import dbldatagen as dg


df_spec = (
     dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000,
                      partitions=4, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumnSpec("email",
                    template=r'\w.\w@\w.com|\w@\w.co.u\k')
    .withColumnSpec("ip_addr",
                     template=r'\n.\n.\n.\n')
    .withColumnSpec("phone",
                     template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
    )

