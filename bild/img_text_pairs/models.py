import torch
import open_clip
from sentence_transformers import SentenceTransformer

class Model:
    def __init__(self) -> None:
        super().__init__()

    def encode_text(self):
        raise NotImplementedError()

    def encode_image(self):
        raise NotImplementedError()

class OpenCLIPModel(Model):
    def __init__(self, device, max_batch_size) -> None:
        model_name = 'ViT-B-32-quickgelu'
        pretrained = 'laion400m_e32'
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(model_name, pretrained=pretrained)
        self.device = device
        self.model.to(device)
        self.tokenizer = open_clip.get_tokenizer(model_name)
        self.max_batch_size = max_batch_size

    def encode_text(self, candidates):
        tokenized_text = self.tokenizer(candidates).to(self.device)

        if tokenized_text.shape[0] > self.max_batch_size:
            num_candidates = tokenized_text.shape[0]
            text_features = torch.zeros([num_candidates, 512]).to(self.device)
            for i in range(0, num_candidates, self.max_batch_size):
                tokenized_text_sub = tokenized_text[i:i + self.max_batch_size]
                text_features[i:i + self.max_batch_size] = self.model.encode_text(tokenized_text_sub)

        else:
            text_features = self.model.encode_text(tokenized_text)

        return text_features

    def encode_image(self, image):
        image = self.preprocess(image).unsqueeze(0).to(self.device)
        return self.model.encode_image(image)

class SentenceTransformerModel(Model):
    def __init__(self, device, max_batch_size) -> None:
        image_model_name = 'clip-ViT-B-32'
        self.image_model = SentenceTransformer(image_model_name, device=device)

        text_model_name = 'sentence-transformers/clip-ViT-B-32-multilingual-v1'
        self.text_model = SentenceTransformer(text_model_name, device=device)

        self.max_batch_size = max_batch_size

    def encode_text(self, candidates):
        return self.text_model.encode(candidates, self.max_batch_size, convert_to_tensor=True)

    def encode_image(self, image):
        return self.image_model.encode([image], convert_to_tensor=True)

def get_model(model_type, device='cuda', max_batch_size=1024):
    if model_type == 'open_clip':
        return OpenCLIPModel(device, max_batch_size)
    elif model_type == 'sentence_transformers':
        return SentenceTransformerModel(device, max_batch_size)
    else:
        raise ValueError("\'model_type\' should be either \'open_clip\' or \'sentence_transformers\'")