package ru.philosophyit.bigdata.entity

case class ReadingConfiguration(
                                 partitionColumn: String,
                                 lowerBound: Long,
                                 upperBound: Long,
                                 numPartitions: Int
                               )
