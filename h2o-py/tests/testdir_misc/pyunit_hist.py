import sys
sys.path.insert(1, "../../")
import h2o

def hist_test(ip,port):
    # Connect to h2o
    h2o.init(ip,port)
    kwargs = {}
    kwargs['server'] = True

    print "Import small prostate dataset"
    hex = h2o.import_frame(h2o.locate("smalldata/logreg/prostate.csv"))

    age_hist = h2o.hist(h2o.H2OFrame(vecs=[hex["AGE"]]), **kwargs)
    vol_hits = h2o.hist(h2o.H2OFrame(vecs=[hex["VOL"]]), **kwargs)

if __name__ == "__main__":
    h2o.run_test(sys.argv, hist_test)