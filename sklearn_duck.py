
import pickle
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
import duckdb
from duckdb.typing import INTEGER, DOUBLE
from duckdb.typing import DuckDBPyType
from sklearn.linear_model import LogisticRegression 
from sklearn import metrics
import pandas as pd

def train_model(X_train, X_test, y_train, y_test):  
    model = LogisticRegression()
    model.fit(X_train,y_train)
    prediction=model.predict(X_test) 
    print('The accuracy of the Logistic Regression is',metrics.accuracy_score(prediction,y_test))
    
    with open('iris-classifier.pkl', 'wb') as f:
        pickle.dump(model, f)

if __name__ == "__main__":
    
    
    iris_dataset = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        iris_dataset['data'], iris_dataset['target'], test_size=0.25, random_state=0)
    
    #train_model(X_train, X_test, y_train, y_test)
      
    
    with open('iris-classifier.pkl', 'rb') as f: 
        model = pickle.load(f)
        
    def duck_sklearn_predict(a:float,b:float,c:float,d:float) -> int:  
        return model.predict([(a,b,c,d)]).item() 
    
   
    iris = pd.DataFrame(X_test)
    iris.columns = ['a','b','c','d'] 
    
 
    conn = duckdb.connect(":memory:") 
    conn.create_function("duck_sklearn_predict", duck_sklearn_predict,[DOUBLE,DOUBLE,DOUBLE,DOUBLE], INTEGER )
    ret = conn.query("""
                     SELECT 
                        a,b,c,d, 
                         duck_sklearn_predict(a,b,c,d) AS sklearn_predict
                    from iris"""
                    ).to_df()
    print(ret.head())
     