import pandas as pd # pandasを読み込む

READ_PATH = "読み込み元CSVパス"  
WRITE_PATH = "書き込み先CSVパス" 


class ChildNodesCount:
    data = pd.read_csv(READ_PATH, encoding='cp932', header=0)  # CSVファイルの読み込み
    nodeKeyCol = 2  # ノードキーが格納されている列番号を記憶
    topLayer = 3  # 最上位層（L1）の列番号を定義
    rightToBottomLayer = 11  # 最下位層（L8）右隣の列番号を定義
    nodeKeyName = {}  # ノードキーとノード名称を格納する配列
    currentParentPerLevel = {}  # ループ処理時点での階層ごとの親を記憶する配列を用意
    childCountPerParent = {}  # ノード毎に子ノード数の数を保持する配列を用意

    for row in data.itertuples():  # 一行ずつ取り出し下記の処理を行う
        currentRow = row  # 処理中の行を現在行に指定
        currentNodeKey = currentRow[nodeKeyCol]  # 処理中のノードキーを変数に記憶
        childCountPerParent[currentNodeKey] = 0  # 子ノード数を初期化
        for j in range(topLayer, rightToBottomLayer):  # L1~L8の間だけ下記の処理を行う
            currentColumn = currentRow[j]  # 処理中の列を変数「currentColumn」に記憶
            if pd.isnull(currentColumn):  # 現在階層にノード名称がない場合、階層を一つ下げる
                j += 1
            else:  # 現在階層にノード名称がある場合
                nodeKeyName[currentNodeKey] = currentColumn  # ノードキーをキーとしてノード名称を配列に格納
                if j == topLayer:  # 処理中のノードが最上位階層（L1）に位置する場合下記の処理を行う
                    currentParentPerLevel[j] = currentNodeKey  # 最上位階層の親ノードを現在ノードとする
                else:
                    k = j - 1  # 1列左の列番号（親ノードが格納されている階層）を変数kに記憶する
                    childCountPerParent[currentParentPerLevel[k]] += 1  # 親ノードの子ノード数カウントを1プラスする処理
                    currentParentPerLevel[j] = currentNodeKey  # 現在階層の親を現在ノードとする
                break

    # 下記はCSV出力処理
    columns = ['NODE_KEY', 'NODE_NAME', 'CHILD_NODES_COUNT']
    df = pd.DataFrame(columns=columns)
    index = 1
    for l, m in childCountPerParent.items():
        # 各種パラメータ設定
        df.at[index, 'NODE_KEY'] = l  # ノードキー
        df.at[index, 'NODE_NAME'] = nodeKeyName[l]  # ノード名称
        df.at[index, 'CHILD_NODES_COUNT'] = m  # 子ノード数カウント
        index += 1

    df.to_csv(WRITE_PATH, encoding='cp932')
