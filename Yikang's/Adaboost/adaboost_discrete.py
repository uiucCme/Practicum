from sklearn.externals.six.moves import zip
import matplotlib.pyplot as plt
import pydotplus
from sklearn.ensemble import AdaBoostClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.tree import DecisionTreeClassifier
import pandas as pd
import numpy as np
import os
import itertools

ALGORITHM_ROUND = 50
TREE_DEPTH = 3


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

def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016/")
data_input_dir = os.getcwd() + "/data/Attributes/"
data_output_dir = os.getcwd() + "/Github Code/Yikang's/Adaboost/"

# read data
print("reading data file...")
data = pd.read_csv(data_input_dir + "30klabeled.csv")
data.drop(0, 0, inplace=True)
Y1 = data['y']  # predict direction by mid price
X = data.drop(['y'], 1)
n_split = 4000

X_train, X_test = X[:n_split], X[n_split:]
Y1_train, Y1_test = Y1[:n_split], Y1[n_split:]

print("start fitting...")
bdt_real = AdaBoostClassifier(
    DecisionTreeClassifier(max_depth=TREE_DEPTH),
    n_estimators=ALGORITHM_ROUND,
    learning_rate=1)
bdt_real.fit(X_train, Y1_train)

bdt_discrete = AdaBoostClassifier(
    DecisionTreeClassifier(max_depth=TREE_DEPTH),
    n_estimators=ALGORITHM_ROUND,
    learning_rate=1.5,
    algorithm="SAMME")
bdt_discrete.fit(X_train, Y1_train)
print('finished fit')


real_test_errors = []
discrete_test_errors = []

for real_test_predict, discrete_train_predict in zip(
        bdt_real.staged_predict(X_test), bdt_discrete.staged_predict(X_test)):
    real_test_errors.append(
        1. - accuracy_score(real_test_predict, Y1_test))
    discrete_test_errors.append(
        1. - accuracy_score(discrete_train_predict, Y1_test))
#
n_trees_discrete = len(bdt_discrete)
n_trees_real = len(bdt_real)

# Boosting might terminate early, but the following arrays are always
# n_estimators long. We crop them to the actual number of trees here:
discrete_estimator_errors = bdt_discrete.estimator_errors_[:n_trees_discrete]
real_estimator_errors = bdt_real.estimator_errors_[:n_trees_real]
discrete_estimator_weights = bdt_discrete.estimator_weights_[:n_trees_discrete]

plt.figure(figsize=(15, 5))

plt.subplot(131)
plt.plot(range(1, n_trees_discrete + 1),
         discrete_test_errors, c='black', label='SAMME')
plt.plot(range(1, n_trees_real + 1),
         real_test_errors, c='black',
         linestyle='dashed', label='SAMME.R')
plt.legend()
plt.ylim(0.18, 0.62)
plt.ylabel('Test Error')
plt.xlabel('Number of Trees')

plt.subplot(122)
plt.plot(range(1, n_trees_discrete + 1), discrete_estimator_errors,
         "b", label='SAMME', alpha=.5)
plt.plot(range(1, n_trees_real + 1), real_estimator_errors,
         "r", label='SAMME.R', alpha=.5)
plt.legend()
plt.ylabel('Error')
plt.xlabel('Number of Trees')
plt.ylim((.2,
          max(real_estimator_errors.max(),
              discrete_estimator_errors.max()) * 1.2))
plt.xlim((-20, len(bdt_discrete) + 20))

plt.subplot(133)
plt.plot(range(1, n_trees_discrete + 1), discrete_estimator_weights,
         "b", label='SAMME')
plt.legend()
plt.ylabel('Weight')
plt.xlabel('Number of Trees')
plt.ylim((0, discrete_estimator_weights.max() * 1.2))
plt.xlim((-20, n_trees_discrete + 20))

# prevent overlapping y-axis labels
plt.subplots_adjust(wspace=0.25)
plt.show()

# Compute confusion matrix
cnf_matrix = confusion_matrix(Y1_test, bdt_real.predict(X_test))
np.set_printoptions(precision=2)

# Plot non-normalized confusion matrix
plt.figure()
plot_confusion_matrix(cnf_matrix, classes=[-2,-1,0,1,2],
                      title='Confusion matrix, without normalization')

# Plot normalized confusion matrix
plt.figure()
plot_confusion_matrix(cnf_matrix, classes=[-2,-1,0,1,2], normalize=True,
                      title='Normalized confusion matrix')

plt.show()

# print('adaboost_real:')
# print(confusion_table(Y1_test, bdt_real.predict(X_test)))
# print('adaboost_discrete')
# print(confusion_table(Y1_test, bdt_discrete.predict(X_test)))