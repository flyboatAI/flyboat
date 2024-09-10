import re


def translate_error_message(origin_except_message):
    if origin_except_message.startswith("Number of features of the model must match the input."):
        str_list: list[str] | None = re.findall(r"\d+", origin_except_message)
        if not str_list or len(str_list) < 2:
            return "模型训练与模型推理特征数量不一致, 请检查配置"
        translate_message = (
            f"模型训练时, 特征数量为{str_list[0]}, " f"模型推理时, 特征数量为{str_list[1]}, 特征数量不一致无法进行推理"
        )
        return translate_message
    if origin_except_message.startswith("The number of features in data"):
        str_list: list[str] | None = re.findall(r"data \((.*?)\)", origin_except_message)
        if not str_list or len(str_list) < 2:
            return "模型训练与模型推理特征数量不一致, 请检查配置"
        return f"模型训练时, 特征数量为{str_list[1]}, 模型推理时, 特征数量为{str_list[0]}, 特征数量不一致无法进行推理"
    if origin_except_message.startswith("Given normalized_shape="):
        str_list: list[str] | None = re.findall(r"\[(.*?)]", origin_except_message)
        if not str_list or len(str_list) < 3:
            return "输入的矩阵形状与期望矩阵形状不一致, 请检查输入配置"
        expect_x_y_list: list[str] | None = str_list[1].split(",")
        real_x_y_list: list[str] | None = str_list[2].split(",")
        if len(expect_x_y_list) != 2 or len(real_x_y_list) != 2:
            return "输入的矩阵形状与期望矩阵形状不一致, 请检查输入配置"
        e_x = "n" if expect_x_y_list[0].strip() == "*" else expect_x_y_list[0].strip()
        e_y = "n" if expect_x_y_list[1].strip() == "*" else expect_x_y_list[1].strip()
        r_x = real_x_y_list[0].strip()
        r_y = real_x_y_list[1].strip()
        return f"期望矩阵为 {e_x} 行 {e_y} 列, 实际输入矩阵为 {r_x} 行 {r_y} 列, 请检查输入配置"
    if origin_except_message.startswith("mat1 and mat2 shapes cannot be multiplied"):
        return "矩阵乘法错误, 请检查算法超参配置"
    if origin_except_message.startswith("Input contains NaN"):
        return "输入数据包含 NAN"
    if origin_except_message.startswith("could not convert string to float:"):
        str_list = re.findall(r"\'(.*)\'", origin_except_message)
        if str_list:
            return f"无法将字符串 {str_list[0]} 转换为浮点数"
        return "无法将字符串转换为浮点数"
    if origin_except_message.startswith("float() argument must be a string or a real number, not "):
        type_list = re.findall(r"\'(.*)\'", origin_except_message)
        if type_list:
            return f"无法将 {type_list[0]} 类型的数据转换为浮点数"
        return "数据无法转换为浮点数"
    if origin_except_message.startswith("unsupported operand type(s) for"):
        # 正则提取个数
        type_list = re.findall(r"\'(.*?)\'", origin_except_message)
        operator = re.findall(r"for (.*):", origin_except_message)
        if type_list and operator:
            return f"{' 与 '.join(type_list)} 无法进行 {operator[0]} 操作"
        return "不受支持的类型操作"
    if origin_except_message.startswith("`n_components` upper bound is"):
        # 正则提取个数
        count_list = re.findall(r"\d+", origin_except_message)
        if count_list and len(count_list) > 1:
            return f"实际输入特征数为 {count_list[0]}, 保存组件个数要求为 {count_list[1]}, 请减少保存组件个数参数值"
        return "请减少保存组件个数参数值"
    if origin_except_message.startswith("Data must have at least"):
        # 正则提取个数
        count_list = re.findall(r"\d+", origin_except_message)
        if count_list:
            return f"输入数据数目至少{count_list[0]}个非空值, 请检查数据"
        return "输入数据数目过少, 请检查数据"
    if origin_except_message == "Data must be positive.":
        return "进行盒式考克斯变换时, 值必须是正数"
    if origin_except_message == "Data must not be constant.":
        return "进行盒式考克斯变换时, 所在列值不能完全一致"
    if origin_except_message.startswith("X has"):
        return "输入数据与模型训练时特征数目不匹配"
    if origin_except_message.startswith("Input X contains NaN."):
        return "预测数据存在空数据, 请在洞察页检查"
    if origin_except_message.startswith(
        "Input X contains infinity or a value too large for dtype"
    ) or origin_except_message.startswith("Input y contains infinity or a value too large for dtype"):
        return "存在 infinity 数据, 或数据值过大"
    if origin_except_message.endswith(
        "the resulting train set will be empty. " "Adjust any of the aforementioned parameters."
    ):
        return "切分后可能导致数据集为空, 无法进行数据切分"
    if origin_except_message.startswith("For multi-task outputs"):
        return "存在多个因变量, 无法训练模型"
    if origin_except_message.startswith("Input contains infinity or a value too large for dtype"):
        return "输入值溢出, 请处理后重试"
    if origin_except_message.startswith("Optimal parameters not found"):
        return "拟合曲线失败, 请查看数据, 使用其他模型再次重试"
    if origin_except_message.startswith("Cannot have number of splits n_splits="):
        # 正则提取个数
        count_list = re.findall(r"\d+", origin_except_message)
        if count_list and len(count_list) == 2:
            return f"样本数据个数为{count_list[1]}, 无法将数据分割为{count_list[0]}份, 请检查数据"
        return "样本数据数目少于要分割的份数, 请检查数据"
    return origin_except_message
