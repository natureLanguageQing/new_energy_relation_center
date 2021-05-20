import re

from keras.layers import Lambda
from keras.models import Model
from tqdm import tqdm

from bert4keras.backend import keras, K
from bert4keras.models import build_transformer_model
from bert4keras.optimizers import Adam
from bert4keras.snippets import sequence_padding, DataGenerator
from bert4keras.tokenizers import Tokenizer, load_vocab
import json
import os

import numpy as np
import pandas as pd


def load_data(filename):
    D = []
    f = pd.read_csv(filename).drop_duplicates().values.tolist()
    for l in f:
        D.append({
            'text': l[3],
            'spo_list': [l[0], l[1], l[2]]
        })
    return D


event_extraction_train_data = load_data('../enterprise_event_extraction/event_extraction.csv')
# 保存一个随机序（供划分valid用）
if not os.path.exists('../random_order.json'):
    random_order = list(range(len(event_extraction_train_data)))
    np.random.shuffle(random_order)
    json.dump(random_order, open('../random_order.json', 'w', encoding="utf-8"), indent=4)
else:
    random_order = json.load(open('../random_order.json'))

# 划分valid
train_data = [event_extraction_train_data[j] for i, j in enumerate(random_order) if i % 3 != 0]
valid_data = [event_extraction_train_data[j] for i, j in enumerate(random_order) if i % 3 == 0]
from company_mrc_train.config import dict_path, checkpoint_path, config_path


def predict_to_file(data, filename):
    """将预测结果输出到文件，方便评估
    """
    with open(filename, 'w', encoding='utf-8') as f:
        for D in tqdm(iter(data), desc=u'正在预测(共%s条样本)' % len(data)):
            question = D['spo_list'][0] + " " + D['spo_list'][1]
            answers = D['spo_list'][2]
            passage = D['text']
            a = gen_answer(question, passage)
            a = max_in_dict(a)
            if a:
                s = u'%s\t%s\t%s\t%s\n' % (question, passage, a, answers)
            else:
                s = u'%s\t%s\t%s\n' % (question, passage, answers)
            f.write(s)
            f.flush()


max_p_len = 64
max_q_len = 32
max_a_len = 16
batch_size = 8
epochs = 10

# 加载并精简词表，建立分词器
token_dict, keep_tokens = load_vocab(
    dict_path=dict_path,
    simplified=True,
    startswith=['[PAD]', '[UNK]', '[CLS]', '[SEP]', '[MASK]'],
)
tokenizer = Tokenizer(token_dict, do_lower_case=True)


class data_generator(DataGenerator):
    """数据生成器
    """

    def for_fit(self):
        while True:
            for d in self.__iter__(True):
                yield d

    def __iter__(self, random=False):
        """单条样本格式为
        输入：[CLS][MASK][MASK][SEP]问题[SEP]篇章[SEP]
        输出：答案
        """
        batch_token_ids, batch_segment_ids, batch_a_token_ids = [], [], []
        for is_end, D in self.sample(random):
            question = D['spo_list'][0] + " " + D['spo_list'][1]
            passage = D['text']
            passage = re.sub(u' |、|；|，', ',', passage)
            final_answer = [D['spo_list'][2]]
            a_token_ids, _ = tokenizer.encode(
                final_answer, maxlen=max_a_len + 1
            )
            q_token_ids, _ = tokenizer.encode(question, maxlen=max_q_len + 1)
            p_token_ids, _ = tokenizer.encode(passage, maxlen=max_p_len + 1)
            token_ids = [tokenizer._token_start_id]
            token_ids += ([tokenizer._token_mask_id] * max_a_len)
            token_ids += [tokenizer._token_end_id]
            token_ids += (q_token_ids[1:] + p_token_ids[1:])
            segment_ids = [0] * len(token_ids)
            batch_token_ids.append(token_ids)
            batch_segment_ids.append(segment_ids)
            batch_a_token_ids.append(a_token_ids[1:])
            if len(batch_token_ids) == self.batch_size or is_end:
                batch_token_ids = sequence_padding(batch_token_ids)
                batch_segment_ids = sequence_padding(batch_segment_ids)
                batch_a_token_ids = sequence_padding(
                    batch_a_token_ids, max_a_len
                )
                yield [batch_token_ids, batch_segment_ids], batch_a_token_ids
                batch_token_ids, batch_segment_ids, batch_a_token_ids = [], [], []


model = build_transformer_model(
    config_path,
    checkpoint_path,
    with_mlm=True,
    keep_tokens=keep_tokens,  # 只保留keep_tokens中的字，精简原字表
)
output = Lambda(lambda x: x[:, 1:max_a_len + 1])(model.output)
model = Model(model.input, output)
model.summary()


def masked_cross_entropy(y_true, y_pred):
    """交叉熵作为loss，并mask掉padding部分的预测
    """
    y_true = K.reshape(y_true, [K.shape(y_true)[0], -1])
    y_mask = K.cast(K.not_equal(y_true, 0), K.floatx())
    cross_entropy = K.sparse_categorical_crossentropy(y_true, y_pred)
    cross_entropy = K.sum(cross_entropy * y_mask) / K.sum(y_mask)
    return cross_entropy


model.compile(loss=masked_cross_entropy, optimizer=Adam(1e-5))


def get_ngram_set(x, n):
    """生成ngram合集，返回结果格式是:
    {(n-1)-gram: set([n-gram的第n个字集合])}
    """
    result = {}
    for i in range(len(x) - n + 1):
        k = tuple(x[i:i + n])
        if k[:-1] not in result:
            result[k[:-1]] = set()
        result[k[:-1]].add(k[-1])
    return result


def gen_answer(question, passages):
    """由于是MLM模型，所以可以直接argmax解码。
    """
    all_p_token_ids, token_ids, segment_ids = [], [], []
    for passage in passages:
        passage = re.sub(u' |、|；|，', ',', passage)
        p_token_ids, _ = tokenizer.encode(passage, maxlen=max_p_len + 1)
        q_token_ids, _ = tokenizer.encode(question, maxlen=max_q_len + 1)
        all_p_token_ids.append(p_token_ids[1:])
        token_ids.append([tokenizer._token_start_id])
        token_ids[-1] += ([tokenizer._token_mask_id] * max_a_len)
        token_ids[-1] += [tokenizer._token_end_id]
        token_ids[-1] += (q_token_ids[1:] + p_token_ids[1:])
        segment_ids.append([0] * len(token_ids[-1]))
    token_ids = sequence_padding(token_ids)
    segment_ids = sequence_padding(segment_ids)
    probas = model.predict([token_ids, segment_ids])
    results = {}
    for t, p in zip(all_p_token_ids, probas):
        a, score = tuple(), 0.
        for i in range(max_a_len):
            idxs = list(get_ngram_set(t, i + 1)[a])
            if tokenizer._token_end_id not in idxs:
                idxs.append(tokenizer._token_end_id)
            # pi是将passage以外的token的概率置零
            pi = np.zeros_like(p[i])
            pi[idxs] = p[i, idxs]
            a = a + (pi.argmax(),)
            score += pi.max()
            if a[-1] == tokenizer._token_end_id:
                break
        score = score / (i + 1)
        a = tokenizer.decode(a)
        if a:
            results[a] = results.get(a, []) + [score]
    results = {
        k: (np.array(v) ** 2).sum() / (sum(v) + 1)
        for k, v in results.items()
    }
    return results


def max_in_dict(d):
    if d:
        return sorted(d.items(), key=lambda s: -s[1])[0][0]


class Evaluate(keras.callbacks.Callback):
    def __init__(self):
        self.lowest = 1e10

    def on_epoch_end(self, epoch, logs=None):
        # 保存最优
        if logs['loss'] <= self.lowest:
            self.lowest = logs['loss']
            predict_to_file(valid_data, "mrc_event_extraction.tsv")
            model.save_weights('./mrc_event_extraction.weights')


if __name__ == '__main__':

    evaluator = Evaluate()
    train_generator = data_generator(train_data, batch_size)

    model.fit_generator(
        train_generator.for_fit(),
        steps_per_epoch=len(train_generator),
        epochs=epochs,
        callbacks=[evaluator]
    )

else:
    if os.path.exists('./mrc_event_extraction.weights'):
        model.load_weights('./mrc_event_extraction.weights')
