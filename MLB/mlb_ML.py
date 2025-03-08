from keras.src.optimizers import SGD
import mlb_database
import pandas as pd
import mlb_pred_v2
import numpy as np
from sklearn.model_selection import train_test_split
from keras import Sequential, layers, models
# pip install tensorflow
MLB_MODEL = r'C:\Users\jamel\PycharmProjects\JamelScripts\MLB\mlb_predictor.keras'


def mlb_database_to_matrix(table):
    query = f'SELECT * FROM {table}'
    connection = mlb_database.create_db_connection(*mlb_database.logon_dict.values(), 'mlb')
    mlb_data = pd.read_sql(query, connection)
    return mlb_data


def load_mlb_model():
    model: Sequential = models.load_model(MLB_MODEL)
    return model


def get_mlb_prediction(game_id, model):
    try:
        _, away_team, home_team, away_ops, home_ops, away_fp, home_fp, away_era, home_era, _ = mlb_pred_v2.get_lineup_data(
            game_id)
        game_info = np.array((away_ops, home_ops, away_fp, home_fp, away_era, home_era), dtype=float).reshape(1, -1)
        prediction = model.predict([game_info])[0][0]
        # print(f'{away_team}:{1-prediction}  {home_team}:{prediction}')
        return away_team, home_team, (1 - prediction, prediction)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    mlb_data = mlb_database_to_matrix('games')
    mlb_data = mlb_data[mlb_data['homeoraway'] != 2]
    X = mlb_data[['away_ops', 'home_ops', 'away_fp', 'home_fp', 'away_era', 'home_era']]
    y = mlb_data['homeoraway']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = Sequential()
    model.add(layers.Dense(12, activation='relu', input_shape=(6,), kernel_initializer='he_uniform'))
    # model.add(layers.Dense(18, activation='relu'))
    model.add(layers.Dense(1, activation='sigmoid'))
    opt = SGD(learning_rate=0.001, momentum=0.9)
    model.compile(loss='binary_crossentropy', optimizer=opt, metrics=['accuracy'])

    model.fit(X_train, y_train, epochs=50, batch_size=10, validation_data=(X_test, y_test))
    _, accuracy = model.evaluate(X_test, y_test)
    print('Accuracy: %.2f' % (accuracy * 100))
    model.save(MLB_MODEL)
    model = models.load_model(MLB_MODEL)
