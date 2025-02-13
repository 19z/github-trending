import os
import re
import threading

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import mysql.connector
from urllib.parse import urlparse
import base64
import time
from dotenv import load_dotenv

load_dotenv()

# 配置环境变量
GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
OPENAI_API_URL = os.environ['OPENAI_API_URL']
OPENAI_MODEL = os.environ['OPENAI_MODEL']
DATABASE_URL = os.environ['DATABASE_URL'] # 'mysql+mysqlconnector://root:root@127.0.0.1:3306/github_trending'
SPOKEN_LANGUAGE = 'any'
LANGUAGE = 'any'

# 解析数据库URL
db_url = urlparse(DATABASE_URL)
db_config = {
    'user': db_url.username,
    'password': db_url.password,
    'host': db_url.hostname,
    'port': db_url.port,
    'database': db_url.path.lstrip('/'),
    'charset': 'utf8mb4'
}

# 初始化数据库连接
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor(buffered=True)

# 创建表结构
cursor.execute("""
CREATE TABLE IF NOT EXISTS github_trending (
    spoken_language VARCHAR(100) DEFAULT 'any',
    language VARCHAR(100) DEFAULT 'any',
    date DATE,
    repository_name VARCHAR(200),
    sort_index INT,
    repo_star INT,
    repo_star_today INT,
    CONSTRAINT uq_trending_entry UNIQUE (spoken_language, language, date, sort_index)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS github_repository (
    name VARCHAR(200) PRIMARY KEY,
    language VARCHAR(100),
    fork_num INT,
    star_num INT,
    license VARCHAR(100),
    last_updated BIGINT,
    created_at BIGINT,
    readme mediumtext,
    about VARCHAR(1000),
    about_link VARCHAR(255),
    ai_summary TEXT,
    last_flush_time BIGINT,
    delete_time BIGINT,
    first_in_trending DATE,
    top_in_trending INT,
    last_in_trending DATE,
    in_trending_time INT DEFAULT 0
)
""")
conn.commit()

# GitHub API配置
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}


def parse_stars(text):
    """解析stars数量"""
    numbers = re.findall(r'[\d,]+', text)
    if numbers:
        return int(numbers[0].replace(',', ''))
    return 0


def fetch_trending_repos():
    """步骤1：获取GitHub趋势数据"""
    print('fetch trending repos: ', SPOKEN_LANGUAGE, LANGUAGE)
    url = 'https://github.com/trending'
    if LANGUAGE != 'any':
        url += f'/{LANGUAGE}'
    if SPOKEN_LANGUAGE != 'any':
        url += f'?spoken_language_code={SPOKEN_LANGUAGE}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    today = datetime.now(timezone.utc).date()
    repos = []
    repos_details = []

    for idx, article in enumerate(soup.select('article.Box-row'), 1):
        repo_name = article.h2.a.get('href').lstrip('/')
        stars_today = parse_stars(
            article.select_one('span.float-sm-right').text if article.select_one('span.float-sm-right') else '')
        language = article.select_one('span[itemprop="programmingLanguage"]')
        repo_language = language.text.strip() if language else ''
        repo_star = int(article.select('a.Link.Link--muted')[0].text.strip().replace(',', ''))
        repo_fork = int(article.select('a.Link.Link--muted')[1].text.strip().replace(',', ''))

        repos.append((
            SPOKEN_LANGUAGE,
            LANGUAGE,
            today,
            repo_name,
            idx,
            repo_star,
            stars_today
        ))
        repos_details.append({
            'repository_name': repo_name,
            'language': repo_language,
            'star_num': repo_star,
            'fork_num': repo_fork,
            'sort_index': idx,
        })

    # 删除当天已有数据
    cursor.execute("""
    DELETE FROM github_trending 
    WHERE date = %s 
      AND spoken_language = %s 
      AND language = %s
    """, (today, SPOKEN_LANGUAGE, LANGUAGE))

    # 批量插入新数据
    insert_query = """
    INSERT INTO github_trending 
    (spoken_language, language, date, repository_name, sort_index, repo_star, repo_star_today)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, repos)
    conn.commit()

    # 更新仓库趋势信息
    update_trending_stats(repos_details)


def update_trending_stats(repos):
    """更新仓库的趋势统计信息"""
    today = datetime.now(timezone.utc).date()

    upsert_query = """
    INSERT INTO github_repository 
    (name, language, star_num, fork_num, first_in_trending, last_in_trending, top_in_trending, in_trending_time)
    VALUES (%(name)s, %(language)s, %(star_num)s, %(fork_num)s, %(first_in_trending)s, 
            %(last_in_trending)s, %(top_in_trending)s, %(in_trending_time)s)
    ON DUPLICATE KEY UPDATE
    star_num = VALUES(star_num),
    fork_num = VALUES(fork_num),
    last_in_trending = VALUES(last_in_trending),
    in_trending_time = IF(last_in_trending = VALUES(last_in_trending), in_trending_time, in_trending_time + 1),
    top_in_trending = LEAST(top_in_trending, VALUES(top_in_trending))
    """

    for repo in repos:
        data = {
            'name': repo['repository_name'],
            'language': repo['language'],
            'star_num': repo['star_num'],
            'fork_num': repo['fork_num'],
            'first_in_trending': today,
            'last_in_trending': today,
            'top_in_trending': repo['sort_index'],
            'in_trending_time': 1
        }
        cursor.execute(upsert_query, data)
    conn.commit()


github_session = requests.Session()

request_lock = threading.Lock()
db_lock = threading.Lock()


def github_api_request(url, params=None):
    """使用GitHub API会话进行请求"""
    with request_lock:
        # 获取当前时间
        current_time = time.time()
        # 检查是否需要等待
        if hasattr(github_api_request, 'last_request_time'):
            time_since_last_request = current_time - github_api_request.last_request_time
            if time_since_last_request < 2:
                time.sleep(2 - time_since_last_request)
        # 记录当前请求时间
        github_api_request.last_request_time = current_time
        with github_session.get(url, headers=HEADERS, params=params) as response:
            return response.json()


def fetch_repo_details(repo_name):
    """步骤2：获取仓库详细信息"""
    try:
        print(f'---- {repo_name}----')
        print('获取基础信息', end=' ')
        repo_info = github_api_request(f'https://api.github.com/repos/{repo_name}')
        if 'message' in repo_info:
            handle_deleted_repo(repo_name)
            return
        print('获取README', end=' ')
        # 获取README
        readme_info = github_api_request(f'https://api.github.com/repos/{repo_name}/readme')
        readme = base64.b64decode(readme_info['content']).decode('utf-8')

        # 处理许可证信息
        license = repo_info.get('license', {}).get('name') if repo_info.get('license') else None

        # 准备更新数据
        update_data = (
            repo_info.get('forks_count'),
            repo_info.get('stargazers_count'),
            license,
            int(datetime.strptime(repo_info['pushed_at'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
            int(datetime.strptime(repo_info['created_at'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
            readme,
            repo_info.get('description'),
            repo_info.get('homepage'),
            int(time.time()),
            repo_name
        )
        with db_lock:
            # 检查内容变化
            cursor.execute("""
            SELECT readme, about, ai_summary 
            FROM github_repository 
            WHERE name = %s
            """, (repo_name,))
            existing = cursor.fetchone()

            # 更新仓库信息
            cursor.execute("""
            UPDATE github_repository SET
                fork_num = %s,
                star_num = %s,
                license = %s,
                last_updated = %s,
                created_at = %s,
                readme = %s,
                about = %s,
                about_link = %s,
                last_flush_time = %s
            WHERE name = %s
            """, update_data)
            conn.commit()

        # 生成AI摘要
        if (existing and (existing[0] != update_data[5] or existing[1] != update_data[6])) \
                or not existing or not existing[2]:
            print('生成摘要', end=' ')
            _summary = generate_ai_summary(repo_name, update_data[6], update_data[5])
            print(_summary)

    except Exception as e:
        print(f"Error processing {repo_name}: {str(e)}")
        conn.rollback()


def generate_ai_summary(repo_name, about, readme):
    """步骤3：生成AI摘要"""
    prompt = f"\n仓库名：{repo_name}\n描述: {about}\n\n----\n<README>\n{readme[:2000]}\n</README>"

    try:
        response = requests.post(
            f"{OPENAI_API_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": OPENAI_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": "根据描述和 README 为该项目生成简洁的 100-200 字中文摘要。"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
        )
        response.raise_for_status()
        summary = response.json()['choices'][0]['message']['content'].strip()
        with db_lock:
            cursor.execute("""
            UPDATE github_repository 
            SET ai_summary = %s 
            WHERE name = %s
            """, (summary, repo_name))
            conn.commit()
        return summary
    except Exception as e:
        print(f"AI summary failed for {repo_name}: {str(e)}")


def handle_deleted_repo(repo_name):
    """处理已删除的仓库"""
    cursor.execute("""
    UPDATE github_repository 
    SET delete_time = %s 
    WHERE name = %s
    """, (int(time.time()), repo_name))
    conn.commit()


def main():
    try:
        # 步骤1：获取趋势数据
        global SPOKEN_LANGUAGE, LANGUAGE
        # for _SPOKEN_LANGUAGE in ['any']:
        #     for _LANGUAGE in 'any/javascript/typescript/java/go/python'.split('/'):
        #         SPOKEN_LANGUAGE = _SPOKEN_LANGUAGE
        #         LANGUAGE = _LANGUAGE
        #         start_time = time.time()
        #         fetch_trending_repos()
        #         end_time = time.time()
        #         if end_time - start_time < 5:
        #             time.sleep(5.1 - end_time + start_time)  # 遵守GitHub API速率限制


        # 步骤2：获取仓库详情
        _now = int(time.time())
        _today = datetime.now(timezone.utc).date()

        cursor.execute("""
        SELECT name 
        FROM github_repository 
        WHERE ai_summary IS NULL 
           OR last_flush_time IS NULL 
           OR (last_flush_time < %s AND last_in_trending = %s)
        """, (_now - 3600 * 24 * 20, _today))

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            executor.map(fetch_repo_details, [repo_name for (repo_name,) in cursor.fetchall()])
            executor.shutdown(wait=True)

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
