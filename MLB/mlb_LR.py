import joblib
import mlb_database
import pandas as pd
import mlb_pred_v2
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

#i can combine regression and NN
MLB_MODEL=r'C:\Users\jamel\PycharmProjects\JamelScripts\MLB\mlb_predictor.pkl'
def mlb_database_to_matrix(table):
    query = f'SELECT * FROM {table}'
    connection=mlb_database.create_db_connection(*mlb_database.logon_dict.values(), 'mlb')
    mlb_data=pd.read_sql(query,connection)
    return mlb_data
def load_mlb_model():
    return joblib.load(MLB_MODEL)
def get_mlb_prediction(game_id,model):
    try:
        _,away_team,home_team,away_ops,home_ops,away_fp,home_fp,away_era,home_era,_=mlb_pred_v2.get_lineup_data(game_id)
        game_info=np.array((away_ops,home_ops,away_fp,home_fp,away_era,home_era),dtype=float).reshape(1,-1)
        prediction=model.predict_proba(game_info)[0]
        # print(f'{away_team}:{prediction[0]}  {home_team}:{prediction[1]}')
        return away_team, home_team,prediction
    except Exception as e:
        print(e)

if __name__=='__main__':
    mlb_data=mlb_database_to_matrix('games')
    mlb_data=mlb_data[mlb_data['homeoraway']!=2]
    X = mlb_data[['away_ops', 'home_ops', 'away_fp', 'home_fp', 'away_era', 'home_era']]
    y = mlb_data['homeoraway']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    logreg = LogisticRegression(random_state=16)

    # fit the model with data
    logreg.fit(X, y)

    joblib.dump(logreg, MLB_MODEL)


