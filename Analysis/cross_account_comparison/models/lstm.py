"""Module for LSTM Modelling"""

import logging
import pandas as pd
import numpy as np
from keras.models import load_model
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Input
from keras.utils import plot_model
from general_functions.conncet_s3 import S3Connection as S3BucketConnectorInnkeepr
from tensorflow.keras.optimizers import Adam


class NNLSTM:
    """
    LSTM Class
    """

    def __init__(
        self,
        params: dict,
        path_to_save: str,
        bucket: S3BucketConnectorInnkeepr,
    ):
        """
        Initialization function
        Args:
            params: dictionary of model params
            path_to_save: path to save
            bucket: S3 bucket connection to use
        """
        self.params = params
        self.path_to_save = path_to_save
        logging.info(f"self.path_to_save = {self.path_to_save}")
        self.bucket = bucket

    def get_weights_func(self, model, path):
        """
        Function to save weihts of LSTM
        Args:
            model: classifier
            path: path to save data to
        """
        names = [weight.name for layer in model.layers for weight in layer.weights]
        print("Names = ", names, flush=True)
        # trainable_weights = [layer.trainable_weights for layer in model.layers]
        # print("trainable_weights = ", train_test_split, flush=True)
        # print(model.layers[0].trainable_weights)
        count = 0
        for ilayer, layer in enumerate(model.layers):
            weights = layer.get_weights()
            name = layer.get_config()["name"]
            for l, w in enumerate(weights):
                # write weights to dataframe
                w = pd.DataFrame(w, dtype="float32").reset_index(drop=True)
                w.columns = [str(col) for col in w.columns]
                path_w = (
                    str(path)
                    + "weight_"
                    + str(name)
                    + "_layer_"
                    + str(ilayer)
                    + "_"
                    + str(l)
                    + ".parquet"
                )
                self.bucket.write_df_to_parquet(w, path_w)
            count += 1

    def define_lstm(self, input_dim, number_samples=None, alpha=None):
        """
        Function to set LSTM parameters
        Args:
            input_dim: number of cols of input
        """
        logging.info(f"Define LSTM with input_dim {input_dim}")
        # define network settings or use defaul metrics
        if "dim_output" in self.params.keys():
            dim_output = self.params["dim_output"]
        else:
            # set default output neuron
            dim_output = 1
        if "learning_rate" in self.params.keys():
            learning_rate = self.params["learning_rate"]
        else:
            learning_rate = 0.001
        if "output_activation_func" in self.params.keys():
            output_activation_func = self.params["output_activation_func"]
        else:
            output_activation_func = "sigmoid"
        if "use_bias" in self.params.keys():
            use_bias = self.params["use_bias"]
        else:
            use_bias = True
        if "lstm_neurons" in self.params.keys():
            lstm_neurons = self.params["lstm_neurons"]
        else:
            if alpha is None:
                alpha = 2
            if number_samples is None:
                raise Exception("number of samples have to be given")
            lstm_neurons = int(number_samples / (alpha * (input_dim + dim_output)))
        # create and fit the LSTM network
        model = Sequential()  # create a Sequental model
        # add LSTM layer with the shape of the input
        # create input array: number of parameters: input_dim * output_dim
        model.add(Input(shape=(None, input_dim), name="Input"))
        # Add a LSTM layer with 128 internal units.
        # Params for lstm: 4 (functions in lstm) * ( (output_dim + lstm_neurons) * lstm_neurons + lstm_neurons (biases))
        model.add(LSTM(lstm_neurons, name="LSTM1", return_sequences=False))
        # For 2 lstm layers
        # model.add(LSTM(lstm_neurons, name="LSTM1", return_sequences=True))
        # model.add(LSTM(int(lstm_neurons*0.7), name="LSTM2"))
        # Add a Dense layer with dense_dim units.
        # Params: lstm_neurons * dense_dim + dense_dim (bias)
        model.add(Dense(dim_output, activation=output_activation_func, name="Output"))
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(
            loss="binary_crossentropy",
            optimizer=optimizer,
            metrics=["accuracy"],  # F1?s
        )
        self.get_weights_func(model, self.path_to_save)
        logging.info(f"define_lstm summary: {model.summary()}")
        name = self.path_to_save.split("/")[-1]
        dir = self.path_to_save.split("weights_init/")[0]
        plot_model(model, to_file=dir + name + "_model_setting.png", show_shapes=True)
        return model

    def load_lstm_model(self, path, compile=False):
        return load_model(path, compile=compile)
