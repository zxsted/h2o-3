setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../h2o-runit.R')

# Constants and setters
bools <- c(TRUE, FALSE)
set_x <- function(cols) {
  if(sample(bools,1)) {
    while (TRUE){
      myX <- cols
      for(i in 1:length(cols))
        if (sample(bools, 1))
          myX <- myX[-i]
      if(length(myX) > 0)
        break
    }
    return(myX)
  } else
    cols
}
set_y <- function(col) return(col)
set_training_frame <- function(frame) return(frame)
set_validation_frame <- function(frame) return(frame)
set_distribution <- function(distribution) return(distribution)
set_ntrees <- function() sample.int(50, 1)
set_max_depth <- function() sample.int(30, 1)
set_min_rows <- function() sample.int(20, 1)
set_learn_rate <- function() runif(1)
set_nbins <- function() sample(2:1000, 1)
set_nbins_cats <- function() {}
set_balance_classes <- function() sample(bools, 1)
set_max_after_balance_size <- function(balance) {
  if (sample(bools, 1))
    return(runif(1, 1, 100))
  else
    return(runif(1, 0.1, 1))
}
set_nfolds <- function() {}
set_score_each_iteration <- function() sample(bools, 1)

randomParams <- function(distribution, train, test, x, y) {
  parms <- list()

  parm_set <- function(parm, required = FALSE, dep = TRUE, ...) {
    if (!dep)
      return(NULL)
    if (required || sample(bools,1)) {
      val <- do.call(paste0("set_", parm), list(...))
      if (!is.null(val))
        if (is.vector(val))
          Log.info(paste0(sub("_", " ", parm), ": ", paste(val, collapse = ", ")))
        else if (inherits(val, "H2OFrame"))
          Log.info(paste0(sub("_", " ", parm), ": ", val@frame_id))
        else
          Log.info(paste0(sub("_", " ", parm), ": ", val))
      return(val)
    }
    return(NULL)
  }

  parms$x <- parm_set("x", required = TRUE, cols = x)
  parms$y <- parm_set("y", required = TRUE, col = y)
  parms$training_frame <- parm_set("training_frame", required = TRUE, frame = train)
  parms$validation_frame <- parm_set("validation_frame", frame = test)
  parms$distribution <- parm_set("distribution", required = TRUE, distribution = distribution)
  parms$ntrees <- parm_set("ntrees")
  parms$max_depth <- parm_set("max_depth")
  parms$min_rows <- parm_set("min_rows")
  parms$learn_rate <- parm_set("learn_rate")
  parms$nbins <- parm_set("nbins")
  # parms$nbins_cats <- parm_set("nbins_cats")
  parms$balance_classes <- parm_set("balance_classes",
    dep = distribution %in% c("multinomial", "bernoulli"))
  parms$max_after_balance_size <- parm_set("max_after_balance_size",
    dep = !is.null(parms$balance_classes) && parms$balance_classes)
  parms$score_each_iteration <- parm_set("score_each_iteration")

  t <- system.time(hh <- do.call("h2o.gbm", parms))
  print(hh)

  h2o.rm(hh@model_id)
  print("#########################################################################################")
  print("")
  print(t)
  print("")

}

test.GBM.rand_attk_forloop <- function(conn) {
  Log.info("Import and data munging...")
  pros.hex <- h2o.uploadFile(conn, locate("smalldata/prostate/prostate.csv"))
  pros.hex[,2] <- as.factor(pros.hex[,2])
  # This as.factor is bugged
  # pros.hex[,4] <- as.factor(pros.hex[,4])
  pros.hex[,5] <- as.factor(pros.hex[,5])
  pros.hex[,6] <- as.factor(pros.hex[,6])
  pros.hex[,9] <- as.factor(pros.hex[,9])
  p.sid <- h2o.runif(pros.hex)
  pros.train <- h2o.assign(pros.hex[p.sid > .2, ], "pros.train")
  pros.test <- h2o.assign(pros.hex[p.sid <= .2, ], "pros.test")

  iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_wheader.csv"))
  i.sid <- h2o.runif(iris.hex)
  iris.train <- h2o.assign(iris.hex[i.sid > .2, ], "iris.train")
  iris.test <- h2o.assign(iris.hex[i.sid <= .2, ], "iris.test")

  cars.hex <- h2o.uploadFile(conn, locate("smalldata/junit/cars.csv"))
  c.sid <- h2o.runif(cars.hex)
  cars.train <- h2o.assign(cars.hex[c.sid > .2, ], "cars.train")
  cars.test <- h2o.assign(cars.hex[c.sid <= .2, ], "cars.test")

  Log.info("### Binomial ###")
  for(i in 1:10)
    randomParams("bernoulli", pros.train, pros.test, 3:9, 2)
  Log.info("### Multinomial ###")
  for(i in 1:10)
    randomParams("multinomial", iris.train, iris.test, 1:4, 5)
  Log.info("### Regression ###")
  for(i in 1:10)
    randomParams("gaussian", cars.train, cars.test, 4:7, 3)

  testEnd()
}

doTest("Checking GBM in Random Attack For Loops", test.GBM.rand_attk_forloop)
