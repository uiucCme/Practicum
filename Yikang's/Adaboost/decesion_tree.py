from IPython.display import Image
from sklearn import tree
import pydotplus
import os
import pandas as pd

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


os.chdir("/Users/luoy2/OneDrive/Documents/UIUC/CME Fall 2016")
data_input_dir = os.getcwd() + "/data/Attributes/"
data_output_dir = os.getcwd() + "/Adaboost/"

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

clf = tree.DecisionTreeClassifier(max_depth=4)
clf = clf.fit(X_train, Y1_train)
dot_data = tree.export_graphviz(clf, out_file=None, filled=True, rounded=True,
                                special_characters=True,
                                feature_names=X_train.columns)
graph = pydotplus.graph_from_dot_data(dot_data)
confusion_table(Y1_train, clf.predict(X_train))
confusion_table(Y1_test, clf.predict(X_test))
graph.write_pdf(data_output_dir + "tree.pdf")
