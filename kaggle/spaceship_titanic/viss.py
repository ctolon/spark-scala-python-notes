# EDA
import pandas as pd

for column_name in train_num_columns:
    g = sns.FacetGrid(train_arrow, col='Transported')
    g.map(plt.hist, column_name, bins=40)
    plt.show()
    
for i in cat_cols:
    print(i)
    sns.barplot(x=train_arrow[i].value_counts().index, y=train_arrow[i].value_counts()).set_title(i)
    plt.show()
    
for i in cat_cols:
    if i != "Transported":
        sns.barplot(train_arrow, x=i, y='Transported').set_title(i)
        plt.show()
        
for i in num_cols:
    sns.histplot(train_arrow[i], kde=True, bins=25).set_title(i)
    plt.show()
    
sns.heatmap(train_arrow.corr(), cmap='winter', annot=True, linecolor='black')