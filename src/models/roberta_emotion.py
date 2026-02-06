
from tqdm import tqdm
from transformers import pipeline
from src import global_configs as cf


class RobertaEmotion:

    def __init__(self, model_name: str, api_token: str | None = None):
        """
        Initializes a class instance with specified model details and sets up a text classification
        pipeline using provided parameters.

        Args:
            model_name (str): Name of the pre-trained model to be used for text classification.
            api_token (str | None): Optional API token for authenticating requests to the model. If
                None, the token will not be used.
        """

        self.model_name = model_name
        self.api_token = api_token
        self.classifier = pipeline(
            task="text-classification",
            model=model_name,
            device_map=cf.DEVICE,
            trust_remote_code=True,
            token=api_token,
            top_k=None,
            truncation=True
        )

    def emotion_classifier(self, sentences: list[str], batch_size: int = 5) -> list[list[dict]]:
        """
        Classifies the emotions present in a list of sentences using a batch processing approach.

        This method leverages an emotion classification model to analyze the input
        sentences in batches, which helps optimize the processing for large datasets
        and ensures efficient utilization of resources. Each batch of sentences
        is passed to the classifier, and the results are aggregated and returned
        as a list of dictionaries containing the classification output.

        Args:
            sentences (list[str]): A list of sentences to classify for emotional content.
            batch_size (int): The number of sentences to process in a single batch. Defaults to 5.

        Returns:
            list[list[dict]]: A list where each entry corresponds to a list of dictionaries representing
            the classified emotions for the respective sentence in the input.
        """

        results = []
        for i in tqdm(range(0, len(sentences), batch_size), desc="Classifying Emotions"):
            batch = sentences[i:i + batch_size]
            batch_results = self.classifier(batch)
            results.extend(batch_results)

        return results
