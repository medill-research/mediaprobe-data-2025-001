
import logging
from pydantic import BaseModel
from typing import Type
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class OllamaClient:

    def __init__(self, url: str, api_key: str | None = None):
        """
        Initializes an instance of the class for interacting with an API.

        This constructor configures a client for asynchronous operations, using the
        provided base URL and optional API key. It establishes a connection to the
        specified API endpoint and prepares the instance for subsequent requests.

        Args:
            url (str): The base URL of the API to connect to.
            api_key (str | None): The API key for authentication, if required.
        """

        self.url = url
        self.api_key = api_key
        self.client = AsyncOpenAI(base_url=url, api_key=api_key)

    @staticmethod
    def message_body(system_prompt: str | None, user_prompt: str) -> list[dict]:
        """
        Creates a formatted message suitable for inference by an LLM model.

        The method constructs a message containing a system-level prompt and a user-level
        prompt. If the system prompt is not provided (`None`), a default system prompt is
        used. The resulting message is structured as a list of dictionaries, each dictionary
        representing a role and its associated content.

        Args:
            system_prompt: Optional system-level prompt providing context or instructions
                for the assistant.
            user_prompt: Prompt or input message provided by the user.

        Returns:
            list[dict]: A list of dictionaries containing the formatted system and user
            messages for LLM inference.
        """

        # If system prompt is empty, create one
        if system_prompt is None:
            system_prompt = "You are a precise and efficient assistant."

        # Create LLM message for inference
        message = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        return message

    async def run_inference(
        self,
        messages: list[dict],
        output_structure: Type[BaseModel] | None,
        max_retries: int,
        **kwargs
    ) -> Type[BaseModel] | str:
        """
        Executes an inference request with either structured or unstructured output, supporting
        retries upon failure, and processes the results accordingly.

        If a structured output is required, the method attempts to parse the response into the
        specified output structure through multiple retries until success or reaching the
        maximum number of retries. If no structured output is specified, the method performs
        a normal inference, returning raw text content.

        Args:
            messages (list[dict]): A list of dictionaries representing the messages to be
                included in the inference. Each dictionary typically contains the role
                (e.g., 'user', 'system') and associated content.
            output_structure (Type[BaseModel] | None): An optional model class derived from
                `BaseModel` that defines the structure of the desired output. If provided,
                the function attempts to parse the response into this structure.
            max_retries (int): The maximum number of retry attempts for structured output
                parsing in case of failures.
            **kwargs: Additional keyword arguments passed to the underlying inference API
                client.

        Returns:
            Type[BaseModel] | str:
                - If `output_structure` is provided, it returns an instance of the
                  specified structure (subclassed from `BaseModel`) containing the parsed
                  results.
                - If `output_structure` is not provided, it returns the content as a raw
                  string extracted from the response.

        Raises:
            Various exceptions may be raised depending on the behavior of the inference
            API client (e.g., connection errors, parsing errors, or invalid output).
        """

        # If requires structured output
        if output_structure:
            current_retry = 0
            parsed_success = False
            response = ""
            results = None

            # Loop through and perform inference until success or reached max retries
            while not parsed_success and current_retry <= max_retries:
                logger.info(f"Attempt {current_retry} to run inference using structured output ...")

                current_retry += 1
                completion = await self.client.beta.chat.completions.parse(
                    messages=messages,
                    response_format=output_structure,
                    **kwargs
                )
                response = completion.choices[0].message

                # Extract content
                if response.parsed:
                    results = response.parsed
                    parsed_success = True
                    logger.info(f"Successfully parsed results from structure output at attempt {current_retry}")

            if not parsed_success:
                results = response.refusal
                logger.info("Unsuccessful in parsing structured output after reaching max attempt.")

        # Otherwise, run normal inference pipeline
        else:
            response = await self.client.chat.completions.create(messages=messages, **kwargs)
            results = response.choices[0].message.content

        return results