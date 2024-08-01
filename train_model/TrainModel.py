from sqlalchemy import create_engine
from sqlalchemy import text

import pandas as pd
import numpy as np

# Import libraries for training and testing model
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score
from sklearn.feature_selection import SelectKBest, chi2

# Libary to save model / load models
import joblib

class TrainModel:

    def __init__(self):
        pass

    def getDistinctCategory(self,engine):
        temp = '''
        select distinct(category) from fact_sch.job_classification_model_tb
        '''
        category_ls = None
        return category_ls

    def getNonTargetData(self,target_category,engine):
        temp = '''
            select job_id, predicted_category as category, concat(title,'. ', description) as description from public.job_categorization_vw 
                where predicted_category != '{}'
            '''
        non_target_query = temp.format(target_category)

        with engine.connect() as con:
            query = text(non_target_query)
            rs = con.execute(query)

            rows = rs.fetchall()

            non_target_data = pd.DataFrame(rows,columns=['job_id','description'])

        # Tag non-target data
        non_target_data['tag'] = 0

        return non_target_data    
    
    def getTargetData(self,target_category,engine):
        temp = '''
            select job_id, predicted_category as category, concat(title,'. ', description) as description from public.corrected_categorization_tb 
                where predicted_category = '{}' and tag = 1
        '''
        target_query = temp.format(target_category)

        with engine.connect() as con:
            query = text(target_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        target_data = pd.DataFrame(rows,columns=['job_id','description'])

        # Tag target data
        target_data['tag'] = 1

        return target_data
    
    def combineData(self,target_data,non_target_data):
        full_data = pd.concat([target_data,non_target_data],axis=0)

        return full_data
    
    def validation_metrics(self,y_test,y_pred):
        accuracy = accuracy_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred)
        recall = recall_score(y_test, y_pred, average='macro')
        f1 = f1_score(y_test, y_pred, average='macro')

        print(f"Accuracy: {accuracy}")
        print(f"Recall: {recall}")
        print(f"F1 Score: {f1}")
        print(cm)

    # Might not need
    def outputIncorrectrResults(self,y_test,y_pred,full_data):
        # Compare predictions with actual labels
        incorrect_indices = [i for i in range(len(y_test)) if y_pred[i] != y_test.iloc[i]]

        # Output incorrect predictions with job IDs
        incorrect_predictions = full_data.iloc[y_test.index[incorrect_indices]]
        incorrect_predictions['predicted_tag'] = y_pred[incorrect_indices]  # Add predicted tags to the dataframe

        # Print or use incorrect predictions with job IDs
        print("Incorrect Predictions with Job IDs:")
        for _, row in incorrect_predictions.iterrows():
            print(f"Job ID: {row['job_id']}")
            print(f"Description: {row['description']}")
            print(f"Actual Tag: {row['tag']}, Predicted Tag: {row['predicted_tag']}")
        print()

    def saveModel(self,model,target_category):
        temp = '../models/{}_classification_model_v2.pkl'
        fileName = temp.format(target_category)
        joblib.dump(model,fileName)

    def saveCountVec(self,vec,target_category):
        temp = '../models/{}_classification_vec_v2.pkl'
        fileName = temp.format(target_category)
        joblib.dump(vec,fileName)

    def checkDistribution(self,y_train,y_test):
        # Calculate distribution of classes (or any relevant feature)
        train_distribution = y_train.value_counts(normalize=True) * 100
        test_distribution = y_test.value_counts(normalize=True) * 100

        # Print distributions
        print("Training Set Distribution (%):")
        print(train_distribution)
        print("\nTesting Set Distribution (%):")
        print(test_distribution)

        # Calculate exact counts of classes (or any relevant feature)
        train_counts = y_train.value_counts()
        test_counts = y_test.value_counts()

        # Print counts
        print("Training Set Counts:")
        print(train_counts)
        print("\nTesting Set Counts:")
        print(test_counts)

    def trainModel(self):
        # Target Category
        target_category = 'Laboratory and Research'

        target_data = self.getTargetData(target_category)
        non_target_data = self.getNonTargetData(target_category)

        full_data = self.combineData(target_data,non_target_data)

        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            full_data['description'], full_data['tag'], test_size=0.2, random_state=42,stratify=full_data['tag'])

        # Transform text data to Count features
        count_vectorizer = CountVectorizer()
        X_train_count = count_vectorizer.fit_transform(X_train)
        X_test_count = count_vectorizer.transform(X_test)

        # Train a Logistic Regression model count vectorizer
        model = LogisticRegression()
        model.fit(X_train_count, y_train)

        # Make predictions
        y_pred = model.predict(X_test_count)

        self.validation_metrics(y_test, y_pred)

        y_pred_prob = model.predict_proba(X_test_count)

        # Adjust threshold
        threshold = 0.3  # You can change this value
        y_pred = (y_pred_prob[:,1] >= threshold).astype(int)