import pandas as pd
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
