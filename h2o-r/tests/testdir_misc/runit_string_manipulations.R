setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../h2o-runit.R')

test.string.manipulation <- function(conn) {
  s1 <- as.h2o(" this is a string ")
  s2 <- as.h2o("this is another string")
  s3 <- as.h2o("this is a longer string")
  s4 <- as.h2o("this is tall, this is taller")

  Log.info("Single and all substitutions...")
  s4 <- h2o.sub("this", "that", s4)
  print(s4)
  expect_identical(s4[1,1], "that is tall, this is taller")
  s4 <- h2o.gsub("tall", "fast", s4)
  print(s4)
  expect_identical(s4[1,1], "that is fast, this is faster")

  Log.info("Trimming...")
  print(s1[1,1])
  expect_identical(s1[1,1], " this is a string ")
  s1 <- h2o.trim(s1)
  expect_identical(s1[1,1], "this is a string")

  ds <- h2o.rbind(s1, s2, s3)
  print(ds)
  splits <- h2o.strsplit(ds, " ")
  print(splits)
  expect_equal(ncol(splits), 5)

  testEnd()
}

doTest("Testing Various String Manipulations", test.string.manipulation)
