from sklearn import tree
import matplotlib.pyplot as plt
import pydotplus
import pandas as pd
import numpy as np
import os
import itertools
from sklearn.metrics import confusion_matrix

_TREE_DEPTH = 20

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



os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
data_input_dir = os.getcwd() + "/data/Attributes/"
data_output_dir = os.getcwd() + "/data/Decision_Tree/"

# read hdf5 file
print("reading hdf5 file...")
data = pd.read_csv(data_input_dir + "trainset_3state_11_10_dynamic_lag.csv")
data.drop(0, 0, inplace=True)
Y1 = data['y']  # predict direction by mid price
X = data.drop(['y'], 1)
n_split = int(2*len(X)/3)

X_train, X_test = X[:n_split], X[n_split:]
Y1_train, Y1_test = Y1[:n_split], Y1[n_split:]

result_table = []
max_depth = 10
for _TREE_DEPTH in range(1,max_depth+1):
    print(_TREE_DEPTH)
    clf = tree.DecisionTreeClassifier(max_depth=_TREE_DEPTH)
    clf = clf.fit(X_train, Y1_train)
    result_table.append([_TREE_DEPTH, clf.score(X_train, Y1_train), clf.score(X_test, Y1_test)])
    dot_data = tree.export_graphviz(clf, out_file=None, filled=True,
                                    rounded=True,
                                    special_characters=True,
                                    feature_names=X_train.columns)
    graph = pydotplus.graph_from_dot_data(dot_data)
    graph.write_pdf(data_output_dir + str(_TREE_DEPTH) + "_tree.pdf")

plt.plot(range(1, max_depth+1),
         [(1 - x[1]) for x in result_table], c='black', label='Train Error')
plt.plot(range(1, max_depth+1),
         [(1 - x[2]) for x in result_table], c='black',
         linestyle='dashed', label='Test Error')
plt.legend()
plt.ylim(0.18, 0.62)
plt.ylabel('Error')
plt.xlabel('Tree Depth')

#
#
clf = tree.DecisionTreeClassifier(max_depth=4)
clf = clf.fit(X_train, Y1_train)
cnf_matrix = confusion_matrix(Y1_test, clf.predict(X_test))
np.set_printoptions(precision=2)

# Plot non-normalized confusion matrix
plt.figure()
plot_confusion_matrix(cnf_matrix, classes=[-2,-1,0,1,2],
                      title='Confusion matrix, without normalization')
plt.show()
# # Plot normalized confusion matrix
# plt.figure()
# plot_confusion_matrix(cnf_matrix, classes=[-2,-1,0,1,2], normalize=True,
#                       title='Normalized confusion matrix')
#
# plt.show()
