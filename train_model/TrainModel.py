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

    # Transform verified data from data_sch and load into sot_sch
    def __convert_to_table_name(self,input_string):
        # Convert to lowercase
        input_string = input_string.lower()
        # Replace spaces with underscores
        table_name = input_string.replace(' ', '_')
        # Add '_tb' suffix
        table_name += '_tb'
        return table_name

    def getDistinctCategory(self,engine):
        temp = '''
        select distinct(category) from data_sch.job_category_prediction_verification_tb
        '''
        with engine.connect() as conn:
            query = text(temp)
            rs = conn.execute(query)
            rows = rs.fetchall()

        distinct_category = pd.DataFrame(rows,columns=['category'])
        return distinct_category
    
    def getTrainingData(self,target_category,engine):
        with engine.connect() as conn:
            with conn.begin() as trans:
                temp = '''
                    select prediction, description from sot_sch.{}
                    '''
                target_query = temp.format(self.__convert_to_table_name(target_category))

                with engine.connect() as con:
                    query = text(target_query)
                    rs = con.execute(query)

                    rows = rs.fetchall()

                target_data = pd.DataFrame(rows,columns=['prediction','description'])

        return target_data
        
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

    def updateJobClassModelTable(self,target_category,max_id,engine):
        model_name = target_category + '_classification_model_v' + str(max_id)
        temp = '''
        insert into fact_sch.job_classification_model_tb (category,version,name)
        values ('{category}',{version},'{name}')
        '''
        insert_statement = temp.format(category=target_category,version=max_id,name=model_name)
        with engine.connect() as conn:
            with conn.begin() as trans:
                conn.execute(text(insert_statement))

        model_name = target_category + '_vec_v' + str(max_id)
        temp = '''
        insert into fact_sch.vectorization_model_tb (category,version,name)
        values ('{category}',{version},'{name}')
        '''
        insert_statement = temp.format(category=target_category,version=max_id,name=model_name)
        with engine.connect() as conn:
            with conn.begin() as trans:
                conn.execute(text(insert_statement))


    def __getLatestClassVersion(self,category,engine):
        with engine.connect() as conn:
            temp = '''
                select max(version) 
                from fact_sch.job_classification_model_tb
                where category = '{}'
                '''
            query = temp.format(category)
            rs = conn.execute(text(query))
            rows = rs.fetchall()

            temp = pd.DataFrame(rows,columns=['max_id'])

            max_id = temp['max_id'][0] + 1

        return max_id
    
    def __getLatestVecVersion(self,category,engine):
        with engine.connect() as conn:
            temp = '''
                select max(version) 
                from fact_sch.vectorization_model_tb
                where category = '{}'
                '''
            query = temp.format(category)
            rs = conn.execute(text(query))
            rows = rs.fetchall()

            temp = pd.DataFrame(rows,columns=['max_id'])

            max_id = temp['max_id'][0] + 1

        return max_id

    def __saveModel(self,model,target_category,engine):
        max_id = self.__getLatestClassVersion(target_category,engine)
        temp = '../job_classification_model/{category}_classification_model_v{version}.pkl'
        fileName = temp.format(category=target_category,version=max_id)
        joblib.dump(model,fileName)

    def __saveCountVec(self,vec,target_category,engine):
        max_id = self.__getLatestVecVersion(target_category,engine)
        temp = '../vectorization_model/{category}_classification_vec_v{version}.pkl'
        fileName = temp.format(category=target_category,version=max_id)
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

    def trainModel(self,target_category,training_data,threshold,engine):
        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            training_data['description'], training_data['prediction'], test_size=0.2, random_state=42,stratify=training_data['prediction'])

        # Transform text data to Count features
        count_vectorizer = CountVectorizer()
        X_train_count = count_vectorizer.fit_transform(X_train)
        X_test_count = count_vectorizer.transform(X_test)

        # Train a Logistic Regression model count vectorizer
        model = LogisticRegression()
        model.fit(X_train_count, y_train)

        # Make predictions
        y_pred = model.predict(X_test_count)
        y_pred_prob = model.predict_proba(X_test_count)
        y_pred = (y_pred_prob[:,1] >= threshold).astype(int)

        self.validation_metrics(y_test, y_pred)

        # Save Models
        self.__saveModel(model,target_category,engine)
        self.__saveCountVec(count_vectorizer,target_category,engine)