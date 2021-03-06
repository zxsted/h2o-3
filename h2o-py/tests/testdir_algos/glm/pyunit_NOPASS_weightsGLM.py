import sys
sys.path.insert(1, "../../../")
import h2o
import random
import copy

def weights_check(ip,port):
    # Connect to h2o
    h2o.init(ip,port)

    def check_same(data1, data2):
        glm1_regression = h2o.glm(x=data1[2:20], y=data1[1])
        glm2_regression = h2o.glm(x=data2[2:21], y=data2[1], weights_column="weights")
        glm1_binomial = h2o.glm(x=data1[1:20], y=data1[0], family="binomial")
        glm2_binomial = h2o.glm(x=data2[1:21], y=data2[0], weights_column="weights", family="binomial")

        assert abs(glm1_regression.mse() - glm2_regression.mse()) < 1e-6, "Expected mse's to be the same, but got {0}, " \
                                                                          "and {1}".format(glm1_regression.mse(),
                                                                                           glm2_regression.mse())
        assert abs(glm1_binomial.auc() - glm2_binomial.auc()) < 1e-6, "Expected auc's to be the same, but got {0}, and " \
                                                                      "{1}".format(glm1_binomial.auc(), glm2_binomial.auc())

    data = [["ab"[random.randint(0,1)] if c==0 else random.gauss(0,1) for c in range(20)] for r in range(100)]
    h2o_data = h2o.H2OFrame(python_obj=data)

    # uniform weights same as no weights
    weight = random.uniform(.1,100)
    uniform_weights = [[weight] for r in range(100)]
    h2o_uniform_weights = h2o.H2OFrame(python_obj=uniform_weights)
    h2o_uniform_weights.setNames(["weights"])
    h2o_data_uniform_weights = h2o.cbind(h2o_data, h2o_uniform_weights)

    print "Checking that using uniform weights is equivalent to no weights:"
    print
    check_same(h2o_data, h2o_data_uniform_weights)

    # zero weights same as removed observations
    zero_weights = [[0] if random.randint(0,1) else [1] for r in range(100)]
    h2o_zero_weights = h2o.H2OFrame(python_obj=zero_weights)
    h2o_zero_weights.setNames(["weights"])
    h2o_data_zero_weights = h2o.cbind(h2o_data, h2o_zero_weights)
    h2o_data_zeros_removed = h2o_data[h2o_zero_weights["weights"] == 1]

    print "Checking that using some zero weights is equivalent to removing those observations:"
    print
    check_same(h2o_data_zeros_removed, h2o_data_zero_weights)

    # doubled weights same as doubled observations
    doubled_weights = [[1] if random.randint(0,1) else [2] for r in range(100)]
    h2o_doubled_weights = h2o.H2OFrame(python_obj=doubled_weights)
    h2o_doubled_weights.setNames(["weights"])
    h2o_data_doubled_weights = h2o.cbind(h2o_data, h2o_doubled_weights)

    doubled_data = copy.deepcopy(data)
    for d, w in zip(data,doubled_weights):
        if w[0] == 2: doubled_data.append(d)
    h2o_data_doubled = h2o.H2OFrame(python_obj=doubled_data)

    print "Checking that doubling some weights is equivalent to doubling those observations:"
    print
    check_same(h2o_data_doubled, h2o_data_doubled_weights)

    # TODO: random weights

    # TODO: all zero weights???

    # TODO: negative weights???

if __name__ == "__main__":
    h2o.run_test(sys.argv, weights_check)