import pandas as pd
from bert4keras.snippets import DataGenerator
from tqdm import tqdm

from company_mrc_train.mrc_event_extraction import gen_answer, max_in_dict


def load_data(filename):
    D = []
    f = pd.read_csv(filename).drop_duplicates().values.tolist()
    for l in f:
        D.append({
            'text': l[3],
            'spo_list': [l[0], l[1], l[2]]
        })
    return D


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
class data_generator(DataGenerator):
    """数据生成器
    """
    def __iter__(self, random=False):
        """单条样本格式：[CLS]篇章[SEP]问题[SEP]答案[SEP]
        """
        batch_token_ids, batch_segment_ids = [], []
        for is_end, D in self.sample(random):
            question = D['spo_list'][0] + " " + D['spo_list'][1]
            passage = D['text']
            passage = re.sub(u' |、|；|，', ',', passage)
            final_answer = [D['spo_list'][2]]
            qa_token_ids, qa_segment_ids = tokenizer.encode(
                question, final_answer, maxlen=max_qa_len + 1
            )
            p_token_ids, p_segment_ids = tokenizer.encode(
                passage, maxlen=max_p_len
            )
            token_ids = p_token_ids + qa_token_ids[1:]
            segment_ids = p_segment_ids + qa_segment_ids[1:]
            batch_token_ids.append(token_ids)
            batch_segment_ids.append(segment_ids)
            if len(batch_token_ids) == self.batch_size or is_end:
                batch_token_ids = sequence_padding(batch_token_ids)
                batch_segment_ids = sequence_padding(batch_segment_ids)
                yield [batch_token_ids, batch_segment_ids], None
                batch_token_ids, batch_segment_ids = [], []
