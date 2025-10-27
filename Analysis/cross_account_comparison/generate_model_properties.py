from Analysis.cross_account_comparison.models.lstm import NNLSTM

path = "/Users/karolinegriesbach/Documents/Innkeepr/Git/evaluation-and-execution-scripts/Analysis/cross_account_comparison/models/junoandme-conversion-6532600c1f8399fb23dee5e6*2025-09-14_best_models*2025-06-16_classifier_NNLSTM_50_nan_nan_nan_saved_model_cv_7_classifier_conversion_probability_model.h5"
weights = "/Users/karolinegriesbach/Documents/Innkeepr/Git/evaluation-and-execution-scripts/Analysis/cross_account_comparison/models/junoandme-conversion-6532600c1f8399fb23dee5e6*2025-09-14_best_models*2025-06-16_classifier_NNLSTM_50_nan_nan_nan_saved_model_cv_7_classifier_conversion_probability_model_weights.weights.h5"

# model = load_lstm_model({}, path, *)
# print(model.summary())

model = NNLSTM({}, path, None)
model = model.load_lstm_model(path, compile=True)
model.load_weights(weights)
print(model)
print(model.summary())
