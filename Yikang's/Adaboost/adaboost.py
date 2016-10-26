from sklearn import tree
import matplotlib.pyplot as plt
import pydotplus
from sklearn.ensemble import AdaBoostClassifier
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
import pandas as pd
import os


ALGORITHM_ROUND = 50
TREE_DEPTH = 2


def confusion_table(true, predict):
    confusion_table = pd.crosstab(true,
                                  predict,
                                  rownames=['True'],
                                  colnames=['Predicted'],
                                  margins=True)
    confusion_table['percentage'] = [
        confusion_table[-1].iloc[0] / confusion_table['All'].iloc[0],
        confusion_table[0].iloc[1] / confusion_table['All'].iloc[1],
        confusion_table[1].iloc[2] / confusion_table['All'].iloc[2],
        (confusion_table[-1].iloc[0] + confusion_table[0].iloc[1] +
         confusion_table[1].iloc[2]) / confusion_table['All'].iloc[3]]
    return confusion_table


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/")
data_input_dir = os.getcwd() + "/data/random_forest/"
data_output_dir = os.getcwd() + "/data/Adaboost/"

# read hdf5 file
print("reading hdf5 file...")
data = pd.read_csv(data_input_dir + "Attributes_10_26_lag1000000.csv")
data.drop(0, 0, inplace=True)
Y1 = data['direction.by.midp']  # predict direction by mid price
Y2 = data['direction.by.e']
X = data.drop(['0', 'mid.price', 'future.mid.price', 'direction.by.midp',
               'direction.by.e'], 1)
n_split = 4000

X_train, X_test = X[:n_split], X[n_split:]
Y1_train, Y1_test = Y1[:n_split], Y1[n_split:]
Y2_train, Y2_test = Y2[:n_split], Y2[n_split:]

print("start fitting...")
bdt_real = AdaBoostClassifier(
    DecisionTreeClassifier(max_depth=TREE_DEPTH),
    n_estimators=ALGORITHM_ROUND,
    learning_rate=1)
bdt_real.fit(X_train, Y1_train)

# bdt_discrete = AdaBoostClassifier(
#     DecisionTreeClassifier(max_depth=2),
#     n_estimators=50,
#     learning_rate=1.5,
#     algorithm="SAMME")
# bdt_discrete.fit(X_train, Y1_train)
print('finished fit')
print(confusion_table(Y1_test, bdt_real.predict(X_test)))


prob = bdt_real.predict_proba(X_test)
pred = Y1_test.tolist()

table = pd.DataFrame(prob)
table = pd.concat([table, pd.DataFrame(pred)], 1)
table.columns = [-1, 0, 1, 'true']
table.sort(1, ascending=False)[:1000]
bdt_real.get_params()

# draw trees in every
round_count = 0
for trees in bdt_real.estimators_:
    dot_data = tree.export_graphviz(trees, out_file=None, filled=True, rounded=True,
                                    special_characters=True,
                                    feature_names=X_train.columns)
    graph = pydotplus.graph_from_dot_data(dot_data)
    graph.write_pdf(data_output_dir + "tree_round_" + str(round_count) + ".pdf")
    round_count += 1
real_test_errors = []
# discrete_test_errors = []

# for real_test_predict, discrete_train_predict in zip(
#         bdt_real.staged_predict(X_test), bdt_discrete.staged_predict(X_test)):
#     real_test_errors.append(
#         1. - accuracy_score(real_test_predict, Y1_test))
#     discrete_test_errors.append(
#         1. - accuracy_score(discrete_train_predict, Y1_test))
#
# n_trees_discrete = len(bdt_discrete)
for real_test_predict in bdt_real.staged_predict(X_test):
    real_test_errors.append(1. - accuracy_score(real_test_predict, Y1_test))
n_trees_real = len(bdt_real)

# Boosting might terminate early, but the following arrays are always
# n_estimators long. We crop them to the actual number of trees here:
# discrete_estimator_errors = bdt_discrete.estimator_errors_[:n_trees_discrete]
real_estimator_errors = bdt_real.estimator_errors_[:n_trees_real]
# discrete_estimator_weights = bdt_discrete.estimator_weights_[:n_trees_discrete]

plt.figure(figsize=(15, 5))

plt.subplot(121)
# plt.plot(range(1, n_trees_discrete + 1),
#          discrete_test_errors, c='black', label='SAMME')
plt.plot(range(1, n_trees_real + 1),
         real_test_errors, c='black',
         linestyle='dashed', label='SAMME.R')
plt.legend()
plt.ylim(0.18, 0.62)
plt.ylabel('Test Error')
plt.xlabel('Number of Trees')

plt.subplot(122)
# plt.plot(range(1, n_trees_discrete + 1), discrete_estimator_errors,
#          "b", label='SAMME', alpha=.5)
plt.plot(range(1, n_trees_real + 1), real_estimator_errors,
         "r", label='SAMME.R', alpha=.5)
plt.legend()
plt.ylabel('Error')
plt.xlabel('Number of Trees')
plt.ylim((.2, real_estimator_errors.max() * 1.2))
# plt.ylim((.2,
#           max(real_estimator_errors.max(),
#               discrete_estimator_errors.max()) * 1.2))
# plt.xlim((-20, len(bdt_discrete) + 20))

# plt.subplot(133)
# plt.plot(range(1, n_trees_discrete + 1), discrete_estimator_weights,
#          "b", label='SAMME')
# plt.legend()
# plt.ylabel('Weight')
# plt.xlabel('Number of Trees')
# plt.ylim((0, discrete_estimator_weights.max() * 1.2))
# plt.xlim((-20, n_trees_discrete + 20))
#
# prevent overlapping y-axis labels
# plt.subplots_adjust(wspace=0.25)
# plt.show()
