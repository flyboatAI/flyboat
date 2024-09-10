import jieba
import torch
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

# 分别加载训练集和测试集


class MyDataset(Dataset):
    def __init__(self, df, feature_col, label_col):
        self.data = df[feature_col].values.flatten().tolist()
        self.label = df[label_col].values.flatten().tolist()

    def __getitem__(self, index):
        data = self.data[index]
        label = self.label[index]
        return data, label

    def __len__(self):
        return len(self.label)


class LoadingClassificationData:
    def __init__(self, batch_size=64, shuffle=True, device="cpu"):
        # self.train_path = train_path
        # self.test_path = test_path
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.device = device

    @staticmethod
    def load_corpus_bert(path):
        """
        加载语料库
        """
        data = []
        with open(path, "r", encoding="utf8") as f:
            for line in f:
                [content, s] = line.split("	", 2)
                content = processing_bert(content)
                data.append((content, int(s)))
        return data

    def data_load(self, train_data, feature_col, label_col):
        # 训练集
        # train_data = self.load_corpus_bert(self.train_path)
        # df_train = pd.DataFrame(train_data, columns=["text", "label"])
        train_data = MyDataset(train_data, feature_col, label_col)
        train_loader = DataLoader(train_data, batch_size=self.batch_size, shuffle=True)

        # 测试集
        # test_data = self.load_corpus_bert(self.test_path)
        # df_test = pd.DataFrame(test_data, columns=["text", "label"])
        # test_data = MyDataset(df_test)
        # test_loader = DataLoader(test_data, batch_size=self.batch_size, shuffle=True)

        return train_loader


def processing_bert(text):
    """
    数据预处理, 可以根据自己的需求进行重载
    """
    # 数据清洗部分
    # text = re.sub("\{%.+?%\}", " ", text)  # 去除 {%xxx%} (地理定位, 微博话题等)
    # text = re.sub("@.+?( |$)", " ", text)  # 去除 @xxx (用户名)
    # text = re.sub("【.+?】", " ", text)  # 去除 【xx】 (里面的内容通常都不是用户自己写的)
    # text = re.sub("\u200b", " ", text)  # '\u200b'是这个数据集中的一个bad case, 不用特别在意
    return text


def processing(text):
    """
    数据预处理, 可以根据自己的需求进行重载
    """
    # 数据清洗部分
    text = processing_bert(text)  # '\u200b'是这个数据集中的一个bad case, 不用特别在意
    # 分词
    words = [w for w in jieba.lcut(text) if w.isalpha()]
    # 对否定词`不`做特殊处理: 与其后面的词进行拼接
    while "不" in words:
        index = words.index("不")
        if index == len(words) - 1:
            break
        words[index : index + 2] = ["".join(words[index : index + 2])]  # 列表切片赋值的酷炫写法
    # 用空格拼接成字符串
    result = " ".join(words)
    return result


def load_corpus_bert(path):
    """
    加载语料库
    """
    data = []
    with open(path, "r", encoding="utf8") as f:
        for line in f:
            [_, s, content] = line.split(",", 2)
            content = processing_bert(content)
            data.append((content, int(s)))
    return data


class TextClassificationPredict:
    def __init__(self, model, bert, tokenizer, df, device="cpu"):
        self.net = model
        self.df = df
        self.device = device
        # 加载
        self.tokenizer = tokenizer
        self.bert = bert


def run(self):
    sentence = []
    result = []
    for i in tqdm(range(len(self.df)), desc="Processing", unit="iteration"):
        vec = self.df.iloc[i].iloc[0]
        sentence.append(vec)
        tokens = self.tokenizer([vec], padding=True)
        input_ids = torch.tensor(tokens["input_ids"]).to(self.device)
        attention_mask = torch.tensor(tokens["attention_mask"]).to(self.device)
        last_hidden_states = self.bert(input_ids, attention_mask=attention_mask)
        bert_output = last_hidden_states[0][:, 0]
        outputs = self.net(bert_output)

        # 获取结果
        r = torch.argmax(outputs).detach().cpu().item()
        result.append(r)

    return result
