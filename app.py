import os
import random
import string
import cloudinary
import cloudinary.uploader
import cloudinary.api
import re
import io
import json
import sys
import requests
import google.generativeai as genai
from PIL import Image
import base64
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, case, or_, text
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from functools import wraps
from collections import defaultdict, deque
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from itertools import product, combinations

# --- 1. アプリケーションとデータベースの初期設定 ---
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 4.5 * 1024 * 1024  # 4.5MB制限
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY') or 'dev_key_sample'
basedir = os.path.abspath(os.path.dirname(__file__))

# Cloudinary設定
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET')
)

database_url = os.environ.get('DATABASE_URL')
if database_url:
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url.replace("postgres://", "postgresql://", 1)
else:
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database.db')

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- 2. ログインマネージャーの設定 ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "このページにアクセスするにはログインが必要です。"

# --- 3. データベースモデル ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256))
    role = db.Column(db.String(20), nullable=False, default='user')
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)
    @property
    def is_admin(self): return self.role == 'admin'

class Season(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    is_current = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    logo_image = db.Column(db.String(255), nullable=True)
    league = db.Column(db.String(50), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    players = db.relationship('Player', backref='team', lazy=True)

class Player(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    image_url = db.Column(db.String(255), nullable=True)

class Game(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=True)
    game_date = db.Column(db.String(50))
    start_time = db.Column(db.String(20), nullable=True)
    game_password = db.Column(db.String(50), nullable=True)
    home_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    away_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    home_score = db.Column(db.Integer, default=0)
    away_score = db.Column(db.Integer, default=0)
    is_finished = db.Column(db.Boolean, default=False)
    
    # ★重要: 不戦敗フラグを追加 (これが無いと保存されません)
    is_forfeit = db.Column(db.Boolean, default=False)
    
    youtube_url_home = db.Column(db.String(200), nullable=True)
    youtube_url_away = db.Column(db.String(200), nullable=True)
    winner_id = db.Column(db.Integer, nullable=True)
    loser_id = db.Column(db.Integer, nullable=True)
    result_input_time = db.Column(db.DateTime, nullable=True)
    result_image_url = db.Column(db.String(500), nullable=True) 
    home_team = db.relationship('Team', foreign_keys=[home_team_id])
    away_team = db.relationship('Team', foreign_keys=[away_team_id])
    season = db.relationship('Season')

class PlayerStat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    pts=db.Column(db.Integer, default=0); ast=db.Column(db.Integer, default=0)
    reb=db.Column(db.Integer, default=0); stl=db.Column(db.Integer, default=0)
    blk=db.Column(db.Integer, default=0); foul=db.Column(db.Integer, default=0)
    turnover=db.Column(db.Integer, default=0); fgm=db.Column(db.Integer, default=0)
    fga=db.Column(db.Integer, default=0); three_pm=db.Column(db.Integer, default=0)
    three_pa=db.Column(db.Integer, default=0); ftm=db.Column(db.Integer, default=0)
    fta=db.Column(db.Integer, default=0)
    player = db.relationship('Player')
    game = db.relationship('Game')

class News(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    image_url = db.Column(db.String(255), nullable=True) 

class PlayoffMatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=True)
    league = db.Column(db.String(20))
    round_name = db.Column(db.String(20))
    match_index = db.Column(db.Integer)
    team1_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=True)
    team2_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=True)
    team1_wins = db.Column(db.Integer, default=0)
    team2_wins = db.Column(db.Integer, default=0)
    schedule_note = db.Column(db.String(50), nullable=True)
    team1 = db.relationship('Team', foreign_keys=[team1_id])
    team2 = db.relationship('Team', foreign_keys=[team2_id])

class VoteConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=True)
    title = db.Column(db.String(100), nullable=False)
    vote_type = db.Column(db.String(20), nullable=False)
    description = db.Column(db.Text)
    start_date = db.Column(db.String(20), nullable=True) 
    end_date = db.Column(db.String(20), nullable=True)
    is_open = db.Column(db.Boolean, default=False)
    is_published = db.Column(db.Boolean, default=False)
    show_on_home = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    votes = db.relationship('Vote', backref='config', lazy=True)
    results = db.relationship('VoteResult', backref='config', lazy=True)
    season = db.relationship('Season')

class Vote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vote_config_id = db.Column(db.Integer, db.ForeignKey('vote_config.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    category = db.Column(db.String(50))
    rank_value = db.Column(db.Integer, default=1)
    user = db.relationship('User')
    player = db.relationship('Player')

class VoteResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    vote_config_id = db.Column(db.Integer, db.ForeignKey('vote_config.id'), nullable=False)
    category = db.Column(db.String(50))
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    score = db.Column(db.Integer)
    rank = db.Column(db.Integer)
    player = db.relationship('Player')

class MVPCandidate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    score = db.Column(db.Float, default=0.0)
    avg_pts = db.Column(db.Float, default=0.0); avg_reb = db.Column(db.Float, default=0.0)
    avg_ast = db.Column(db.Float, default=0.0); avg_stl = db.Column(db.Float, default=0.0)
    avg_blk = db.Column(db.Float, default=0.0); fg_pct = db.Column(db.Float, default=0.0)
    three_pt_pct = db.Column(db.Float, default=0.0)
    league_name = db.Column(db.String(50))
    candidate_type = db.Column(db.String(20), default='weekly')
    team_wins = db.Column(db.Integer, default=0)
    team_losses = db.Column(db.Integer, default=0)
    player = db.relationship('Player')

class SystemSetting(db.Model):
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.String(255))

# --- 4. 権限管理とヘルパー関数 ---
Team_Home = db.aliased(Team, name='team_home') 
Team_Away = db.aliased(Team, name='team_away')

@login_manager.user_loader
def load_user(user_id): return User.query.get(int(user_id))

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("権限がありません。"); return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename): return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif'}
def generate_password(length=4): return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def get_current_season():
    season = Season.query.filter_by(is_current=True).first()
    if not season:
        season = Season.query.order_by(Season.id.desc()).first()
        if not season:
            season = Season(name="2K26 Season 1", is_current=True)
            db.session.add(season); db.session.commit()
    return season

def get_view_season_id():
    sid = request.args.get('season_id', type=int)
    if sid: return sid
    return get_current_season().id

@app.context_processor
def inject_seasons():
    return dict(
        all_seasons=Season.query.order_by(Season.id.desc()).all(),
        current_season=get_current_season(),
        view_season_id=get_view_season_id()
    )

def calculate_standings(season_id, league_filter=None):
    # 1. チーム一覧取得
    query = Team.query
    if league_filter:
        query = query.filter(Team.league == league_filter)
    teams = query.all()

    standings = []

    for team in teams:
        # 2. このチームの終了済み全試合を取得
        team_games = Game.query.filter(
            Game.season_id == season_id,
            Game.is_finished == True,
            or_(Game.home_team_id == team.id, Game.away_team_id == team.id)
        ).order_by(Game.game_date.asc()).all()
        
        # --- 集計用変数の初期化 ---
        wins = 0; losses = 0
        points = 0  # 勝ち点
        
        # スタッツ計算用の有効試合リスト（不戦試合を除外）
        valid_game_ids = []
        
        form_history = []
        streak_type = ""; streak_count = 0
        
        pf = 0; pa = 0 # 総得点、総失点

        # --- A. 試合ごとの勝敗・勝ち点計算 ---
        for g in team_games:
            is_home = (g.home_team_id == team.id)
            
            # 勝敗判定: winner_idがあればそれを優先、なければスコアで判定
            if g.winner_id is not None:
                is_win = (g.winner_id == team.id)
            else:
                my_score = g.home_score if is_home else g.away_score
                opp_score = g.away_score if is_home else g.home_score
                is_win = (my_score > opp_score)

            # ★最重要修正: 不戦試合の判定ロジック強化
            # 「フラグがTrue」 または 「スコアが 0-0 (過去データ互換)」 の場合を不戦試合とする
            flag_forfeit = getattr(g, 'is_forfeit', False)
            score_zero_forfeit = (g.home_score == 0 and g.away_score == 0)
            is_treat_as_forfeit = (flag_forfeit or score_zero_forfeit)

            # 勝ち点計算
            if is_win:
                wins += 1
                points += 3 # 不戦勝も通常勝利も3点
                form_history.append('W')
                current_result = "W"
            else:
                losses += 1
                form_history.append('L')
                current_result = "L"
                
                # 不戦敗（判定がTrue）なら0点、通常敗戦なら1点
                if is_treat_as_forfeit:
                    points += 0
                else:
                    points += 1

            # 連勝・連敗
            if streak_type == current_result:
                streak_count += 1
            else:
                streak_type = current_result
                streak_count = 1

            # ★有効試合判定: 不戦試合とみなしたものはリストに入れない（＝スタッツ計算から除外）
            if not is_treat_as_forfeit:
                valid_game_ids.append(g.id)
                # スコア加算
                if is_home:
                    pf += g.home_score; pa += g.away_score
                else:
                    pf += g.away_score; pa += g.home_score

        # --- B. 詳細スタッツの一括集計 ---
        t_ast = 0; t_reb = 0; t_stl = 0; t_blk = 0; t_to = 0; t_foul = 0
        t_fgm = 0; t_fga = 0; t_3pm = 0; t_3pa = 0; t_ftm = 0; t_fta = 0

        # 有効試合が1つ以上ある場合のみ集計
        if valid_game_ids:
            stats_data = db.session.query(
                func.sum(PlayerStat.ast).label('ast'), func.sum(PlayerStat.reb).label('reb'),
                func.sum(PlayerStat.stl).label('stl'), func.sum(PlayerStat.blk).label('blk'),
                func.sum(PlayerStat.turnover).label('to'), func.sum(PlayerStat.foul).label('foul'),
                func.sum(PlayerStat.fgm).label('fgm'), func.sum(PlayerStat.fga).label('fga'),
                func.sum(PlayerStat.three_pm).label('3pm'), func.sum(PlayerStat.three_pa).label('3pa'),
                func.sum(PlayerStat.ftm).label('ftm'), func.sum(PlayerStat.fta).label('fta')
            ).join(Player, PlayerStat.player_id == Player.id)\
             .filter(
                 PlayerStat.game_id.in_(valid_game_ids), # 有効試合のみ対象
                 Player.team_id == team.id               # このチームの選手のみ
             ).first()

            if stats_data:
                t_ast = stats_data.ast or 0
                t_reb = stats_data.reb or 0
                t_stl = stats_data.stl or 0
                t_blk = stats_data.blk or 0
                t_to  = stats_data.to  or 0
                t_foul= stats_data.foul or 0
                t_fgm = stats_data.fgm or 0
                t_fga = stats_data.fga or 0
                t_3pm = stats_data[8] or 0 
                t_3pa = stats_data[9] or 0
                t_ftm = stats_data.ftm or 0
                t_fta = stats_data.fta or 0

        # --- C. 最終計算 ---
        total_games_played = wins + losses      # 順位表上の試合数 (不戦含む)
        valid_games_count = len(valid_game_ids) # 平均計算用の分母 (不戦除く)
        
        # 休部中かつ試合数0のチームは除外
        if not team.is_active and total_games_played == 0:
            continue

        # 平均・得失点差の計算 (分母は有効試合数)
        if valid_games_count > 0:
            avg_pf = pf / valid_games_count
            avg_pa = pa / valid_games_count
            
            avg_ast = t_ast / valid_games_count
            avg_reb = t_reb / valid_games_count
            avg_stl = t_stl / valid_games_count
            avg_blk = t_blk / valid_games_count
            avg_to  = t_to  / valid_games_count
            avg_foul= t_foul/ valid_games_count
        else:
            avg_pf = 0; avg_pa = 0
            avg_ast = 0; avg_reb = 0; avg_stl = 0; avg_blk = 0; avg_to = 0; avg_foul = 0

        # 成功率 (試投数ベース)
        fg_pct = (t_fgm / t_fga * 100) if t_fga > 0 else 0
        three_p_pct = (t_3pm / t_3pa * 100) if t_3pa > 0 else 0
        ft_pct = (t_ftm / t_fta * 100) if t_fta > 0 else 0

        diff = pf - pa
        
        form_str = "-".join(reversed(form_history[-5:]))
        streak_str = f"{streak_type}{streak_count}" if total_games_played > 0 else "-"

        standings.append({
            'team': team,
            'team_name': team.name,
            'league': team.league,
            'wins': wins,
            'losses': losses,
            'points': points,
            'avg_pf': avg_pf,
            'avg_pa': avg_pa,
            'diff': diff,
            'form': form_str,
            'streak': streak_str,
            'avg_ast': avg_ast, 'avg_reb': avg_reb, 'avg_stl': avg_stl,
            'avg_blk': avg_blk, 'avg_turnover': avg_to, 'avg_foul': avg_foul,
            'fg_pct': fg_pct, 'three_p_pct': three_p_pct, 'ft_pct': ft_pct
        })

    # 並び替え: 勝ち点 > 得失点差 > 平均得点
    standings.sort(key=lambda x: (x['points'], x['diff'], x['avg_pf']), reverse=True)
    
    return standings

def calculate_team_stats(season_id):
    # トップページと同じロジックを使うことで整合性を保つ
    return calculate_standings(season_id)

def get_stats_leaders(season_id):
    leaders = {}
    stat_fields = {'pts': '平均得点', 'ast': '平均アシスト', 'reb': '平均リバウンド', 'stl': '平均スティール', 'blk': '平均ブロック'}
    for field_key, field_name in stat_fields.items():
        # 有効試合（不戦試合でない）のみ対象にする
        avg_stat = func.avg(getattr(PlayerStat, field_key)).label('avg_value')
        query_result = db.session.query(Player.name, avg_stat, Player.id)\
            .join(PlayerStat, PlayerStat.player_id == Player.id)\
            .join(Game, PlayerStat.game_id == Game.id)\
            .filter(Game.is_finished == True, Game.season_id == season_id, Game.is_forfeit == False)\
            .group_by(Player.id).order_by(db.desc('avg_value')).limit(5).all()
        leaders[field_name] = query_result
    return leaders

def generate_round_robin_rounds(team_list, reverse_fixtures=False):
    if not team_list or len(team_list) < 2: return []
    local_teams = list(team_list)
    if len(local_teams) % 2 != 0: local_teams.append(None)
    num_teams = len(local_teams); num_rounds = num_teams - 1; all_rounds_games = []; rotating_teams = deque(local_teams[1:])
    for _ in range(num_rounds):
        current_round_games = []
        t1 = local_teams[0]; t2 = rotating_teams[-1]
        if t1 is not None and t2 is not None:
            if reverse_fixtures: current_round_games.append((t2, t1))
            else: current_round_games.append((t1, t2))
        for i in range((num_teams // 2) - 1):
            t1 = rotating_teams[i]; t2 = rotating_teams[-(i + 2)]
            if t1 is not None and t2 is not None:
                if reverse_fixtures: current_round_games.append((t2, t1))
                else: current_round_games.append((t1, t2))
        all_rounds_games.append(current_round_games); rotating_teams.rotate(1)
    return all_rounds_games

def analyze_stats(target_id, all_data, id_key, fields_config, limit=5):
    result = {}
    data_list = []
    for item in all_data:
        entry = {}
        if isinstance(item, dict):
            current_id = item.get('team').id if 'team' in item else item.get(id_key)
            for field in fields_config.keys(): entry[field] = item.get(field, 0) or 0
        else:
            current_id = getattr(item, id_key)
            for field in fields_config.keys(): entry[field] = getattr(item, field, 0) or 0
        entry['id'] = current_id
        data_list.append(entry)

    for field, config in fields_config.items():
        values = [d[field] for d in data_list]
        avg_val = sum(values) / len(values) if values else 0
        target_val = next((d[field] for d in data_list if d['id'] == target_id), 0)
        reverse_sort = not config.get('reverse', False)
        sorted_values = sorted(values, reverse=reverse_sort)
        try: rank = sorted_values.index(target_val) + 1
        except ValueError: rank = len(values)
        if rank <= limit: color_class = 'stat-top' 
        elif (not config.get('reverse', False) and target_val >= avg_val) or (config.get('reverse', False) and target_val <= avg_val): color_class = 'stat-good' 
        else: color_class = 'stat-avg' 
        result[field] = {'value': target_val, 'rank': rank, 'avg': avg_val, 'color_class': color_class, 'label': config['label']}
    return result

# =========================================================
# 5. ルーティング
# =========================================================

# --- マイグレーション（起動時） ---
with app.app_context():
    db.create_all()
    try:
        with db.engine.connect() as conn:
            conn.execute(text("ALTER TABLE news ADD COLUMN image_url VARCHAR(255)"))
            conn.execute(text("ALTER TABLE team ADD COLUMN is_active BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE player ADD COLUMN is_active BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE vote_config ADD COLUMN show_on_home BOOLEAN DEFAULT FALSE"))
            conn.execute(text("ALTER TABLE vote_config ADD COLUMN start_date VARCHAR(20)"))
            conn.execute(text("ALTER TABLE vote_config ADD COLUMN end_date VARCHAR(20)"))
            conn.execute(text("ALTER TABLE mvp_candidate ADD COLUMN candidate_type VARCHAR(20) DEFAULT 'weekly'"))
            # ★追加: Gameテーブルに is_forfeit カラムを追加するマイグレーション
            conn.execute(text("ALTER TABLE game ADD COLUMN is_forfeit BOOLEAN DEFAULT FALSE"))
            conn.execute(text("ALTER TABLE mvp_candidate ADD COLUMN team_wins INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE mvp_candidate ADD COLUMN team_losses INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE player ADD COLUMN image_url VARCHAR(255)"))
    except: pass

# --- ★追加: カード画像アップロード用API ---
@app.route('/api/upload_card', methods=['POST'])
def upload_card():
    data = request.json
    image_data = data.get('image')
    if not image_data:
        return jsonify({'error': 'No image data'}), 400
    
    if 'data:image/png;base64,' in image_data:
        image_data = image_data.replace('data:image/png;base64,', '')
    
    try:
        import base64
        image_binary = base64.b64decode(image_data)
        upload_result = cloudinary.uploader.upload(
            io.BytesIO(image_binary), 
            resource_type="image",
            folder="nba2k_jpl_cards" 
        )
        return jsonify({'url': upload_result['secure_url']})
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user is None or not user.check_password(request.form['password']):
            flash('ユーザー名またはパスワードが無効です'); return redirect(url_for('login'))
        login_user(user); return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout(): logout_user(); flash('ログアウトしました。'); return redirect(url_for('index'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        auth_password = request.form.get('auth_password')
        if auth_password != 'JPL': flash('認証パスワードが違います。'); return redirect(url_for('register'))
        username = request.form['username']
        if User.query.filter_by(username=username).first(): flash("そのユーザー名は既に使用されています。"); return redirect(url_for('register'))
        role = 'admin' if User.query.count() == 0 else 'user'
        new_user = User(username=username, role=role); new_user.set_password(request.form['password'])
        db.session.add(new_user); db.session.commit(); flash(f"ユーザー登録が完了しました。"); return redirect(url_for('login'))
    return render_template('register.html')

# --- 管理: シーズン管理 ---
@app.route('/admin/season', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_season():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'create':
            name = request.form.get('season_name')
            if name:
                Season.query.update({Season.is_current: False})
                new_season = Season(name=name, is_current=True)
                db.session.add(new_season)
                db.session.commit()
                flash(f'新シーズン「{name}」を開始しました！過去のデータはアーカイブされました。')
        elif action == 'switch':
            season_id = request.form.get('season_id')
            Season.query.update({Season.is_current: False})
            target = Season.query.get(season_id)
            if target:
                target.is_current = True
                db.session.commit()
                flash(f'現在のシーズンを「{target.name}」に切り替えました。')
        elif action == 'rename':
            season_id = request.form.get('season_id')
            new_name = request.form.get('new_name')
            target = Season.query.get(season_id)
            if target and new_name:
                target.name = new_name
                db.session.commit()
                flash(f'シーズン名を「{new_name}」に変更しました。')
    seasons = Season.query.order_by(Season.id.desc()).all()
    return render_template('admin_season.html', seasons=seasons)

# --- お知らせ管理 ---
@app.route('/admin/news', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_news():
    if request.method == 'POST':
        action = request.form.get('action')
        
        # ★追加: 速報バーの更新処理
        if action == 'update_ticker':
            text = request.form.get('ticker_text')
            is_active = 'true' if request.form.get('ticker_active') else 'false'
            
            # テキスト保存
            setting_text = SystemSetting.query.get('ticker_text')
            if not setting_text:
                setting_text = SystemSetting(key='ticker_text', value=text)
                db.session.add(setting_text)
            else:
                setting_text.value = text
            
            # ステータス保存
            setting_active = SystemSetting.query.get('ticker_active')
            if not setting_active:
                setting_active = SystemSetting(key='ticker_active', value=is_active)
                db.session.add(setting_active)
            else:
                setting_active.value = is_active
            
            db.session.commit()
            flash('ニュース速報バーの設定を更新しました。')

        elif action == 'add_news':
            title = request.form.get('news_title')
            content = request.form.get('news_content')
            image_url = None
            if 'news_image' in request.files:
                file = request.files['news_image']
                if file and file.filename != '' and allowed_file(file.filename):
                    try:
                        upload_result = cloudinary.uploader.upload(file)
                        image_url = upload_result.get('secure_url')
                    except Exception as e:
                        flash(f"画像アップロードに失敗しました: {e}")
                        return redirect(url_for('admin_news'))
            elif request.form.get('news_image_url'):
                image_url = request.form.get('news_image_url')
            if title and content:
                new_item = News(title=title, content=content, image_url=image_url)
                db.session.add(new_item); db.session.commit()
                flash(f'お知らせ「{title}」を投稿しました。')
            else: flash('タイトルと内容を入力してください。')
        elif action == 'delete_news':
            news_id = request.form.get('news_id')
            news_item = News.query.get(news_id)
            if news_item:
                db.session.delete(news_item); db.session.commit()
                flash('お知らせを削除しました。')
        return redirect(url_for('admin_news'))
    
    news_items = News.query.order_by(News.created_at.desc()).all()
    
    # ★追加: 現在の速報設定を取得してテンプレートへ渡す
    ticker_text_obj = SystemSetting.query.get('ticker_text')
    ticker_active_obj = SystemSetting.query.get('ticker_active')
    current_ticker_text = ticker_text_obj.value if ticker_text_obj else ""
    current_ticker_active = True if ticker_active_obj and ticker_active_obj.value == 'true' else False

    return render_template('admin_news.html', news_items=news_items, ticker_text=current_ticker_text, ticker_active=current_ticker_active)

@app.route('/news/<int:news_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_news(news_id):
    news_item = News.query.get_or_404(news_id)
    if request.method == 'POST':
        news_item.title = request.form.get('news_title')
        news_item.content = request.form.get('news_content')
        if request.form.get('news_image_url'):
            news_item.image_url = request.form.get('news_image_url')
        db.session.commit(); flash('お知らせを更新しました。'); return redirect(url_for('admin_news'))
    return render_template('edit_news.html', news_item=news_item)

# --- プレイオフ管理 ---
@app.route('/admin/playoff', methods=['GET', 'POST'])
@login_required
def admin_playoff():
    if not current_user.is_admin: return redirect(url_for('index'))
    season = get_current_season()
    if PlayoffMatch.query.filter_by(season_id=season.id).count() == 0:
        rounds = [('A', '1st Round', 4), ('A', 'Semi Final', 2), ('A', 'Conf Final', 1), ('B', '1st Round', 4), ('B', 'Semi Final', 2), ('B', 'Conf Final', 1), ('Final', 'Grand Final', 1)]
        for lg, r_name, count in rounds:
            for i in range(1, count + 1):
                db.session.add(PlayoffMatch(season_id=season.id, league=lg, round_name=r_name, match_index=i))
        db.session.commit()
    matches = PlayoffMatch.query.filter_by(season_id=season.id).order_by(
        PlayoffMatch.league, 
        case((PlayoffMatch.round_name == '1st Round', 1), (PlayoffMatch.round_name == 'Semi Final', 2), (PlayoffMatch.round_name == 'Conf Final', 3), (PlayoffMatch.round_name == 'Grand Final', 4), else_=5),
        PlayoffMatch.match_index
    ).all()
    teams = Team.query.filter_by(is_active=True).order_by(Team.name).all()
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'save_matches':
            for m in matches:
                t1_id = request.form.get(f'team1_{m.id}'); t2_id = request.form.get(f'team2_{m.id}')
                w1 = request.form.get(f'wins1_{m.id}'); w2 = request.form.get(f'wins2_{m.id}')
                m.team1_id = int(t1_id) if t1_id else None; m.team2_id = int(t2_id) if t2_id else None
                m.team1_wins = int(w1) if w1 else 0; m.team2_wins = int(w2) if w2 else 0
                m.schedule_note = request.form.get(f'note_{m.id}')
            db.session.commit(); flash('トーナメント情報を更新しました')
        elif action == 'toggle_visibility':
            current_val = request.form.get('current_visibility'); new_val = 'false' if current_val == 'true' else 'true'
            setting = SystemSetting.query.get('show_playoff')
            if not setting: setting = SystemSetting(key='show_playoff', value=new_val); db.session.add(setting)
            else: setting.value = new_val
            db.session.commit(); flash(f"プレイオフ表の表示を {'ON' if new_val=='true' else 'OFF'} にしました。")
        return redirect(url_for('admin_playoff'))
    setting = SystemSetting.query.get('show_playoff')
    is_visible = True if setting and setting.value == 'true' else False
    return render_template('admin_playoff.html', matches=matches, teams=teams, is_visible=is_visible)

# --- MVP計算用ヘルパー関数 (mvp_selectorの直前に配置してください) ---
def get_team_record_in_period(team_id, start_date, end_date):
    """ 指定期間におけるチームの勝敗数を計算する """
    games = Game.query.filter(
        Game.is_finished == True,
        Game.game_date >= start_date,
        Game.game_date <= end_date,
        or_(Game.home_team_id == team_id, Game.away_team_id == team_id)
    ).all()
    
    w = 0; l = 0
    for g in games:
        is_home = (g.home_team_id == team_id)
        
        # 勝敗判定 (winner_idがあれば優先、なければスコア)
        if g.winner_id is not None:
            is_win = (g.winner_id == team_id)
        else:
            my_score = g.home_score if is_home else g.away_score
            opp_score = g.away_score if is_home else g.home_score
            is_win = (my_score > opp_score)
        
        if is_win: w += 1
        else: l += 1
    return w, l

# --- MVP選出ルート関数 ---
@app.route('/mvp_selector', methods=['GET', 'POST'])
@login_required
@admin_required
def mvp_selector():
    top_players_a = []; top_players_b = []
    start_date = None; end_date = None
    target_type = 'weekly'
    
    setting = SystemSetting.query.get('show_mvp')
    is_mvp_visible = True if setting and setting.value == 'true' else False
    
    if request.method == 'POST':
        action = request.form.get('action')
        target_type = request.form.get('target_type', 'weekly')

        if action == 'calculate' or action == 'publish':
            start_date_str = request.form.get('start_date')
            end_date_str = request.form.get('end_date')
            
            if start_date_str and end_date_str:
                start_date = start_date_str
                end_date = end_date_str
                
                # インパクトスコア計算式
                impact_score = (
                    func.avg(PlayerStat.pts) + func.avg(PlayerStat.reb) + func.avg(PlayerStat.ast) + 
                    func.avg(PlayerStat.stl) + func.avg(PlayerStat.blk) - func.avg(PlayerStat.turnover) - 
                    (func.avg(PlayerStat.fga) - func.avg(PlayerStat.fgm)) - (func.avg(PlayerStat.fta) - func.avg(PlayerStat.ftm))
                )
                # 成功率計算
                fg_pct_calc = case((func.sum(PlayerStat.fga) > 0, func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga)), else_=0.0)
                three_pt_pct_calc = case((func.sum(PlayerStat.three_pa) > 0, func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa)), else_=0.0)
                
                # 共通クエリ作成関数
                def query_candidates(league_name):
                    return db.session.query(
                        Player, Team, 
                        func.count(PlayerStat.game_id).label('games_played'), 
                        impact_score.label('score'), 
                        func.avg(PlayerStat.pts).label('avg_pts'), 
                        func.avg(PlayerStat.reb).label('avg_reb'), 
                        func.avg(PlayerStat.ast).label('avg_ast'), 
                        func.avg(PlayerStat.stl).label('avg_stl'), 
                        func.avg(PlayerStat.blk).label('avg_blk'), 
                        fg_pct_calc.label('fg_pct'), 
                        three_pt_pct_calc.label('three_pt_pct')
                    ).join(PlayerStat, Player.id == PlayerStat.player_id)\
                     .join(Team, Player.team_id == Team.id)\
                     .join(Game, PlayerStat.game_id == Game.id)\
                     .filter(
                         Game.game_date >= start_date, 
                         Game.game_date <= end_date, 
                         Team.league == league_name
                     )\
                     .group_by(Player.id, Team.id)\
                     .having(func.count(PlayerStat.game_id) >= 1)\
                     .order_by(db.desc('score')).limit(5).all()

                if action == 'calculate':
                    # 計算プレビュー用 (テンプレートに渡すデータを作成)
                    raw_a = query_candidates("Aリーグ")
                    raw_b = query_candidates("Bリーグ")
                    
                    def attach_record(raw_list):
                        processed = []
                        for row in raw_list:
                            # row = (Player, Team, games_played, score, ...)
                            player = row[0]; team = row[1]
                            # ★ここで期間中のチーム勝敗を取得
                            w, l = get_team_record_in_period(team.id, start_date, end_date)
                            
                            processed.append({
                                'player': player, 'player_id': player.id, 'team': team,
                                'score': row.score, 'avg_pts': row.avg_pts, 'avg_reb': row.avg_reb, 
                                'avg_ast': row.avg_ast, 'avg_stl': row.avg_stl, 'avg_blk': row.avg_blk, 
                                'fg_pct': row.fg_pct, 'three_pt_pct': row.three_pt_pct,
                                'team_wins': w, 'team_losses': l # テンプレートで表示
                            })
                        return processed

                    top_players_a = attach_record(raw_a)
                    top_players_b = attach_record(raw_b)
                    
                    if not top_players_a and not top_players_b: 
                        flash('指定期間にデータがありません。')

                elif action == 'publish':
                    # 公開用 (DBに保存)
                    MVPCandidate.query.filter_by(candidate_type=target_type).delete()
                    
                    def save_for_league(league_name):
                        results = query_candidates(league_name)
                        for r in results:
                            team_id = r[1].id
                            # ★ここで期間中のチーム勝敗を取得して保存
                            w, l = get_team_record_in_period(team_id, start_date, end_date)
                            
                            candidate = MVPCandidate(
                                player_id=r[0].id, 
                                score=r.score, 
                                avg_pts=r.avg_pts, avg_reb=r.avg_reb, avg_ast=r.avg_ast, 
                                avg_stl=r.avg_stl, avg_blk=r.avg_blk, 
                                fg_pct=(r.fg_pct or 0.0), three_pt_pct=(r.three_pt_pct or 0.0), 
                                league_name=league_name, 
                                candidate_type=target_type,
                                team_wins=w, team_losses=l # DBカラムに追加
                            )
                            db.session.add(candidate)
                            
                    save_for_league("Aリーグ")
                    save_for_league("Bリーグ")
                    
                    setting = SystemSetting.query.get('show_mvp')
                    if not setting: 
                        setting = SystemSetting(key='show_mvp', value='true')
                        db.session.add(setting)
                    else: 
                        setting.value = 'true'
                    
                    db.session.commit()
                    flash(f'{target_type.capitalize()} MVP候補をトップページに公開しました！')
                    return redirect(url_for('index'))

        elif action == 'toggle_visibility':
            current_val = request.form.get('current_visibility')
            new_val = 'false' if current_val == 'true' else 'true'
            setting = SystemSetting.query.get('show_mvp')
            if not setting: 
                setting = SystemSetting(key='show_mvp', value=new_val)
                db.session.add(setting)
            else: 
                setting.value = new_val
            db.session.commit()
            flash(f"表示を {'ON' if new_val=='true' else 'OFF'} にしました。")
            return redirect(url_for('mvp_selector'))

    return render_template('mvp_selector.html', 
                           top_players_a=top_players_a, 
                           top_players_b=top_players_b, 
                           start_date=start_date, 
                           end_date=end_date, 
                           is_mvp_visible=is_mvp_visible, 
                           target_type=target_type)

# --- ロスター管理 ---
@app.route('/roster', methods=['GET', 'POST'])
@login_required
@admin_required
def roster():
    if request.method == 'POST':
        action = request.form.get('action')
        
        # 1. チーム追加
        if action == 'add_team':
            team_name = request.form.get('team_name'); league = request.form.get('league')
            logo_url = None
            if 'logo_image' in request.files:
                file = request.files['logo_image']
                if file and file.filename != '' and allowed_file(file.filename):
                    try:
                        upload_result = cloudinary.uploader.upload(file); logo_url = upload_result.get('secure_url')
                    except Exception as e: flash(f"画像アップロードに失敗しました: {e}"); return redirect(url_for('roster'))
            if team_name and league:
                if not Team.query.filter_by(name=team_name).first():
                    new_team = Team(name=team_name, league=league, logo_image=logo_url)
                    db.session.add(new_team); db.session.commit(); flash(f'チーム「{team_name}」が登録されました。')
                    for i in range(1, 11):
                        p_name = request.form.get(f'player_name_{i}')
                        if p_name: db.session.add(Player(name=p_name, team_id=new_team.id))
                    db.session.commit()
                else: flash(f'チーム「{team_name}」は既に存在します。')
            else: flash('チーム名とリーグを選択してください。')
        
        # 2. 選手追加
        elif action == 'add_player':
            p_name = request.form.get('player_name'); t_id = request.form.get('team_id')
            if p_name and t_id:
                db.session.add(Player(name=p_name, team_id=t_id)); db.session.commit(); flash(f'選手「{p_name}」が登録されました。')
            else: flash('選手名とチームを選択してください。')

        # 3. ユーザー権限変更
        elif action == 'promote_user':
            username_to_promote = request.form.get('username_to_promote')
            if username_to_promote:
                user_to_promote = User.query.filter_by(username=username_to_promote).first()
                if user_to_promote:
                    if user_to_promote.role != 'admin':
                        user_to_promote.role = 'admin'; db.session.commit(); flash(f'ユーザー「{username_to_promote}」を管理者に昇格させました。')
                    else: flash(f'ユーザー「{username_to_promote}」は既に管理者です。')
                else: flash(f'ユーザー「{username_to_promote}」が見つかりません。')
            else: flash('ユーザー名を入力してください。')

        # 4. 選手名編集
        elif action == 'update_player_name':
            player_id = request.form.get('player_id', type=int)
            new_name = request.form.get('new_name')
            player = Player.query.get(player_id)
            if player and new_name:
                player.name = new_name
                db.session.commit()
                # flash(f'選手名を「{new_name}」に変更しました。')

        # 5. 移籍
        elif action == 'transfer_player':
            player_id = request.form.get('player_id', type=int); new_team_id = request.form.get('new_team_id', type=int)
            player = Player.query.get(player_id); new_team = Team.query.get(new_team_id)
            if player and new_team:
                old_team_name = player.team.name
                player.team_id = new_team_id; db.session.commit()
                flash(f'選手「{player.name}」を{old_team_name}から{new_team.name}に移籍させました。')

        # 6. ロゴ更新
        elif action == 'update_logo':
            team_id = request.form.get('team_id', type=int); team = Team.query.get(team_id)
            if not team: flash('対象のチームが見つかりません。'); return redirect(url_for('roster'))
            if 'logo_image' in request.files:
                file = request.files['logo_image']
                if file and file.filename != '' and allowed_file(file.filename):
                    try:
                        if team.logo_image:
                            public_id = os.path.splitext(team.logo_image.split('/')[-1])[0]
                            cloudinary.uploader.destroy(public_id)
                        upload_result = cloudinary.uploader.upload(file); logo_url = upload_result.get('secure_url')
                        team.logo_image = logo_url; db.session.commit(); flash(f'チーム「{team.name}」のロゴを更新しました。')
                    except Exception as e: flash(f"ロゴの更新に失敗しました: {e}")
                elif file.filename != '': flash('許可されていないファイル形式です。')
            else: flash('ロゴファイルが選択されていません。')
        
        # 7. チーム活動切り替え
        elif action == 'toggle_team_active':
            team = Team.query.get(request.form.get('team_id'))
            if team:
                team.is_active = not team.is_active
                db.session.commit()
                flash(f'チーム「{team.name}」を{"活動再開" if team.is_active else "廃部(非表示)"}にしました。')

        # 8. 選手活動切り替え
        elif action == 'toggle_player_active':
            player = Player.query.get(request.form.get('player_id'))
            if player:
                player.is_active = not player.is_active
                db.session.commit()
                flash(f'選手「{player.name}」を{"活動再開" if player.is_active else "引退(非表示)"}にしました。')

        # 9. チーム完全削除
        elif action == 'delete_team':
            if request.form.get('confirm_delete') == 'delete':
                t = Team.query.get(request.form.get('team_id'))
                if t:
                    try:
                        # 1. プレイオフのマッチアップからこのチームを外す (削除せず空にする)
                        # これをやらないと外部キーエラーになります
                        PlayoffMatch.query.filter(PlayoffMatch.team1_id == t.id).update({PlayoffMatch.team1_id: None, PlayoffMatch.team1_wins: 0}, synchronize_session=False)
                        PlayoffMatch.query.filter(PlayoffMatch.team2_id == t.id).update({PlayoffMatch.team2_id: None, PlayoffMatch.team2_wins: 0}, synchronize_session=False)

                        # 2. このチームに関連する選手データを全て削除
                        players = Player.query.filter_by(team_id=t.id).all()
                        for p in players:
                            # 選手に紐づくスタッツ、MVP候補、投票データなどを先に消す
                            PlayerStat.query.filter_by(player_id=p.id).delete()
                            MVPCandidate.query.filter_by(player_id=p.id).delete()
                            Vote.query.filter_by(player_id=p.id).delete()
                            VoteResult.query.filter_by(player_id=p.id).delete()
                            db.session.delete(p)

                        # 3. このチームが関わる試合データを削除
                        games = Game.query.filter(or_(Game.home_team_id == t.id, Game.away_team_id == t.id)).all()
                        for g in games:
                            # 試合に紐づくスタッツを消してから試合を消す
                            PlayerStat.query.filter_by(game_id=g.id).delete()
                            db.session.delete(g)

                        # 4. 最後にチーム自体を削除
                        db.session.delete(t)
                        db.session.commit()
                        flash(f'チーム「{t.name}」を関連データごと完全削除しました。')
                    
                    except Exception as e:
                        db.session.rollback()
                        # エラー内容を画面に表示する
                        flash(f'削除中にエラーが発生しました: {e}')
                        print(f"Delete Error: {e}")
            else:
                flash('確認コードが一致しません。削除をキャンセルしました。')

        # 10. 選手完全削除
        elif action == 'delete_player':
            if request.form.get('confirm_delete') == 'delete':
                p = Player.query.get(request.form.get('player_id'))
                if p:
                    PlayerStat.query.filter_by(player_id=p.id).delete()
                    db.session.delete(p); db.session.commit(); flash(f'選手「{p.name}」を完全削除しました。')
            else: flash('確認コードが一致しません。削除をキャンセルしました。')

        # ★追加: チームのリーグ個別変更
        elif action == 'change_league':
            team_id = request.form.get('team_id')
            new_league = request.form.get('new_league')
            target_team = Team.query.get(team_id)
            if target_team and new_league:
                target_team.league = new_league
                db.session.commit()
                flash(f'チーム「{target_team.name}」を{new_league}へ移動しました。')

        # ★追加: リーグ自動シャッフル
        elif action == 'shuffle_leagues':
            if request.form.get('confirm_shuffle') == 'yes':
                active_teams = Team.query.filter_by(is_active=True).all()
                if len(active_teams) < 2:
                    flash('チーム数が足りないためシャッフルできません。')
                else:
                    import random
                    random.shuffle(active_teams) # ランダムに並び替え
                    
                    mid_index = (len(active_teams) + 1) // 2 # 奇数の場合はAリーグが1つ多くなる
                    
                    for i, t in enumerate(active_teams):
                        if i < mid_index:
                            t.league = 'Aリーグ'
                        else:
                            t.league = 'Bリーグ'
                    
                    db.session.commit()
                    flash(f'全{len(active_teams)}チームのリーグをランダムに振り分けました（A/B均等）。')

        return redirect(url_for('roster'))
    
    teams = Team.query.order_by(Team.league, Team.name).all()
    users = User.query.all()
    return render_template('roster.html', teams=teams, users=users)

@app.route('/add_schedule', methods=['GET', 'POST'])
@login_required
@admin_required
def add_schedule():
    if request.method == 'POST':
        season = get_current_season()
        new_game = Game(
            season_id=season.id,
            game_date=request.form['game_date'], start_time=request.form['start_time'], 
            home_team_id=request.form['home_team_id'], away_team_id=request.form['away_team_id'], 
            game_password=request.form.get('game_password')
        )
        db.session.add(new_game); db.session.commit()
        flash("新しい試合日程が追加されました。"); return redirect(url_for('schedule'))
    teams = Team.query.filter_by(is_active=True).all()
    return render_template('add_schedule.html', teams=teams)

# --- ★修正: 戻り値を「前半戦」と「後半戦」に分けて返すように変更 ---
def create_intra_league_schedule(teams):
    """
    チームリストを受け取り、総当たり2回戦のラウンドリストを
    (前半戦リスト, 後半戦リスト) のタプルで返す。
    """
    if not teams or len(teams) < 2:
        return [], []

    # チーム数が奇数の場合、ダミー(None)を入れて偶数にする
    rotation_teams = list(teams)
    if len(rotation_teams) % 2 != 0:
        rotation_teams.append(None)
    
    n = len(rotation_teams)
    rounds_leg1 = [] # 前半戦
    rounds_leg2 = [] # 後半戦

    # Circle Methodで前半戦を作成
    fixed_team = rotation_teams[0]
    rotating = deque(rotation_teams[1:])

    for r in range(n - 1):
        round_matches = []
        t1 = fixed_team
        t2 = rotating[0]
        
        if t1 is not None and t2 is not None:
            if r % 2 == 0: round_matches.append((t1, t2))
            else: round_matches.append((t2, t1))
        
        for i in range(1, n // 2):
            t_a = rotating[i]
            t_b = rotating[-(i)]
            if t_a is not None and t_b is not None:
                if r % 2 == 0: round_matches.append((t_a, t_b))
                else: round_matches.append((t_b, t_a))
        
        rounds_leg1.append(round_matches)
        rotating.rotate(1)

    # 後半戦を作成（前半戦のH/A入替）
    for round_matches in rounds_leg1:
        reverse_round = []
        for (home, away) in round_matches:
            reverse_round.append((away, home))
        rounds_leg2.append(reverse_round)

    # ★変更: 結合せず、分けて返す
    return rounds_leg1, rounds_leg2

# --- ★追加: 交流戦（1回戦のみ）を作成する関数 ---
def create_inter_league_schedule(teams_a, teams_b):
    """
    リーグAとリーグBの交流戦ラウンドリストを返す
    """
    if not teams_a or not teams_b:
        return []
    
    rounds = []
    # シンプルにリストをずらして対戦させる
    # Aのチーム数分（またはBのチーム数分）のラウンドができる
    
    # チーム数が違う場合の調整（少ない方に合わせるか、Noneで埋めるかだが、今回は16チーム想定でそのまま）
    list_a = list(teams_a)
    deque_b = deque(teams_b)
    
    max_rounds = len(deque_b) # 相手チームの数だけラウンドがある
    
    for r in range(max_rounds):
        current_round = []
        for i, team_a in enumerate(list_a):
            team_b = deque_b[i % len(deque_b)]
            
            # H/Aのバランス調整: 
            # ラウンド偶数回はAホーム、奇数回はBホームにする等で分散
            if r % 2 == 0:
                current_round.append((team_a, team_b))
            else:
                current_round.append((team_b, team_a))
                
        rounds.append(current_round)
        deque_b.rotate(1) # Bリーグをずらす
        
    return rounds


@app.route('/auto_schedule', methods=['GET', 'POST'])
@login_required
@admin_required
def auto_schedule():
    if request.method == 'POST':
        start_date_str = request.form.get('start_date')
        weekdays = request.form.getlist('weekdays')
        times_str = request.form.get('times')
        
        if not all([start_date_str, weekdays, times_str]):
            flash('すべての項目を入力してください。')
            return redirect(url_for('auto_schedule'))
            
        all_teams = Team.query.filter_by(is_active=True).all()
        league_a = [t for t in all_teams if t.league == 'Aリーグ']
        league_b = [t for t in all_teams if t.league == 'Bリーグ']
        
        if not league_a or not league_b:
            flash('チームがAリーグ・Bリーグに正しく設定されていません。')
            return redirect(url_for('auto_schedule'))

        # --- 1. スケジュール生成フェーズ ---
        final_rounds = []

        # (1) 各リーグの日程を「前半・後半」に分けて取得
        sched_a_leg1, sched_a_leg2 = create_intra_league_schedule(league_a)
        sched_b_leg1, sched_b_leg2 = create_intra_league_schedule(league_b)
        
        # (2) 交流戦の日程を取得
        inter_schedule = create_inter_league_schedule(league_a, league_b)

        # --- 【フェーズ1】 同リーグ前半戦 (AとBをマージ) ---
        max_leg1 = max(len(sched_a_leg1), len(sched_b_leg1))
        for i in range(max_leg1):
            combined_round = []
            if i < len(sched_a_leg1): combined_round.extend(sched_a_leg1[i])
            if i < len(sched_b_leg1): combined_round.extend(sched_b_leg1[i])
            if combined_round: final_rounds.append(combined_round)

        # --- 【フェーズ2】 交流戦 (そのまま追加) ---
        final_rounds.extend(inter_schedule)

        # --- 【フェーズ3】 同リーグ後半戦 (AとBをマージ) ---
        max_leg2 = max(len(sched_a_leg2), len(sched_b_leg2))
        for i in range(max_leg2):
            combined_round = []
            if i < len(sched_a_leg2): combined_round.extend(sched_a_leg2[i])
            if i < len(sched_b_leg2): combined_round.extend(sched_b_leg2[i])
            if combined_round: final_rounds.append(combined_round)

        # --- 2. 日時割り当てフェーズ ---
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        selected_weekdays = [int(d) for d in weekdays]
        time_list = [t.strip() for t in times_str.split(',') if t.strip()]
        
        current_date = start_date
        time_index = 0
        games_created_count = 0
        
        alphabet = 'abcdefghijklmnopqrstuvwxyz'
        password_index = 0
        season = get_current_season()
        
        for round_matches in final_rounds:
            # 日付を進める
            while current_date.weekday() not in selected_weekdays:
                current_date += timedelta(days=1)
            
            assigned_date = current_date.strftime('%Y-%m-%d')
            assigned_time = time_list[time_index]
            
            # 時間枠を進める
            time_index += 1
            if time_index >= len(time_list):
                time_index = 0
                current_date += timedelta(days=1)

            # ラウンド内の試合順をシャッフル
            random.shuffle(round_matches)

            for (home_team, away_team) in round_matches:
                game_password = (alphabet[password_index % len(alphabet)] * 6)
                password_index += 1
                
                new_game = Game(
                    season_id=season.id,
                    game_date=assigned_date,
                    start_time=assigned_time,
                    home_team_id=home_team.id,
                    away_team_id=away_team.id,
                    game_password=game_password
                )
                db.session.add(new_game)
                games_created_count += 1
        
        db.session.commit()
        flash(f'全{games_created_count}試合を作成しました。（順序: 前半戦→交流戦→後半戦）')
        return redirect(url_for('schedule'))
        
    return render_template('auto_schedule.html')
@app.route('/schedule')
def schedule():
    view_sid = get_view_season_id()
    selected_team_id = request.args.get('team_id', type=int)
    selected_date = request.args.get('selected_date')
    
    # 日付順（昇順）
    query = Game.query.filter(Game.season_id == view_sid).order_by(Game.game_date.asc(), Game.start_time.asc())
    
    if selected_team_id:
        query = query.filter((Game.home_team_id == selected_team_id) | (Game.away_team_id == selected_team_id))
    if selected_date:
        query = query.filter(Game.game_date == selected_date)
        
    games = query.all()
    all_teams = Team.query.order_by(Team.name).all()
    
    # ★追加: 今日の日付を取得して渡す
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    return render_template('schedule.html', 
                           games=games, 
                           all_teams=all_teams, 
                           selected_team_id=selected_team_id, 
                           selected_date=selected_date,
                           today_str=today_str) # ←これを追加

@app.route('/team/<int:team_id>')
def team_detail(team_id):
    view_sid = get_view_season_id()
    team = Team.query.get_or_404(team_id)
    
    # チーム全体のスタッツ計算（順位表用データの流用）
    all_team_stats_data = calculate_standings(view_sid) 
    target_team_stats = next((item for item in all_team_stats_data if item['team'].id == team_id), {
        'wins': 0, 'losses': 0, 'points': 0, 'diff': 0, 'avg_pf': 0, 'avg_pa': 0, 'avg_reb': 0, 'avg_ast': 0, 'avg_stl': 0, 'avg_blk': 0, 'avg_turnover': 0, 'avg_foul': 0, 'fg_pct': 0, 'three_p_pct': 0, 'ft_pct': 0
    })
    
    # レーダーチャート用データ
    team_fields = {
        'points': {'label': '勝ち点'}, 'avg_pf': {'label': '平均得点'}, 'avg_pa': {'label': '平均失点', 'reverse': True}, 'diff': {'label': '得失点差'},
        'fg_pct': {'label': 'FG%'}, 'three_p_pct': {'label': '3P%'}, 'ft_pct': {'label': 'FT%'},
        'avg_reb': {'label': 'リバウンド'}, 'avg_ast': {'label': 'アシスト'}, 'avg_stl': {'label': 'スティール'},
        'avg_blk': {'label': 'ブロック'}, 'avg_turnover': {'label': 'ターンオーバー', 'reverse': True}, 'avg_foul': {'label': 'ファウル', 'reverse': True},
    }
    analyzed_stats = analyze_stats(team_id, all_team_stats_data, 'none', team_fields, limit=5)
    
    # --- ★修正箇所: 選手リスト取得ロジック ---
    # 1. まず該当シーズンのスタッツを集計するサブクエリを作成
    stats_sub = db.session.query(
        PlayerStat.player_id,
        func.count(PlayerStat.game_id).label('games_played'),
        func.sum(PlayerStat.pts).label('total_pts'),
        func.sum(PlayerStat.reb).label('total_reb'),
        func.sum(PlayerStat.ast).label('total_ast'),
        func.sum(PlayerStat.stl).label('total_stl'),
        func.sum(PlayerStat.blk).label('total_blk'),
        func.sum(PlayerStat.fgm).label('total_fgm'),
        func.sum(PlayerStat.fga).label('total_fga'),
        func.sum(PlayerStat.three_pm).label('total_3pm'),
        func.sum(PlayerStat.three_pa).label('total_3pa'),
        func.sum(PlayerStat.ftm).label('total_ftm'),
        func.sum(PlayerStat.fta).label('total_fta')
    ).join(Game, PlayerStat.game_id == Game.id)\
     .filter(Game.season_id == view_sid)\
     .group_by(PlayerStat.player_id).subquery()

    # 2. チームの全選手を取得し、サブクエリと外部結合(LEFT JOIN)する
    # これにより、スタッツがない選手もリストに含まれるようになる
    player_stats_list = db.session.query(
        Player,
        func.coalesce(stats_sub.c.games_played, 0).label('games_played'),
        # 平均値の計算 (NULL回避)
        case((func.coalesce(stats_sub.c.games_played, 0) > 0, stats_sub.c.total_pts / stats_sub.c.games_played), else_=0).label('avg_pts'),
        case((func.coalesce(stats_sub.c.games_played, 0) > 0, stats_sub.c.total_reb / stats_sub.c.games_played), else_=0).label('avg_reb'),
        case((func.coalesce(stats_sub.c.games_played, 0) > 0, stats_sub.c.total_ast / stats_sub.c.games_played), else_=0).label('avg_ast'),
        case((func.coalesce(stats_sub.c.games_played, 0) > 0, stats_sub.c.total_stl / stats_sub.c.games_played), else_=0).label('avg_stl'),
        case((func.coalesce(stats_sub.c.games_played, 0) > 0, stats_sub.c.total_blk / stats_sub.c.games_played), else_=0).label('avg_blk'),
        # 成功率の計算 (ゼロ除算回避)
        case((func.coalesce(stats_sub.c.total_fga, 0) > 0, stats_sub.c.total_fgm * 100.0 / stats_sub.c.total_fga), else_=0).label('fg_pct'),
        case((func.coalesce(stats_sub.c.total_3pa, 0) > 0, stats_sub.c.total_3pm * 100.0 / stats_sub.c.total_3pa), else_=0).label('three_p_pct'),
        case((func.coalesce(stats_sub.c.total_fta, 0) > 0, stats_sub.c.total_ftm * 100.0 / stats_sub.c.total_fta), else_=0).label('ft_pct')
    ).outerjoin(stats_sub, Player.id == stats_sub.c.player_id)\
     .filter(Player.team_id == team_id)\
     .order_by(Player.name.asc()).all()
     
    # 試合日程
    team_games = Game.query.filter(
        Game.season_id == view_sid,
        or_(Game.home_team_id == team_id, Game.away_team_id == team_id)
    ).order_by(Game.game_date.asc(), Game.start_time.asc()).all()
    
    players = Player.query.filter_by(team_id=team_id).all()
    return render_template('team_detail.html', team=team, players=players, player_stats_list=player_stats_list, team_games=team_games, team_stats=target_team_stats, stats=analyzed_stats)

@app.route('/player/<int:player_id>')
def player_detail(player_id):
    view_sid = get_view_season_id()
    player = Player.query.get_or_404(player_id)
    
    # 1. 選手の通算スタッツ取得
    all_players_stats = db.session.query(
        Player.id.label('player_id'), func.count(PlayerStat.game_id).label('games_played'),
        func.avg(PlayerStat.pts).label('avg_pts'), func.avg(PlayerStat.reb).label('avg_reb'),
        func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.stl).label('avg_stl'),
        func.avg(PlayerStat.blk).label('avg_blk'), func.avg(PlayerStat.turnover).label('avg_turnover'),
        func.avg(PlayerStat.foul).label('avg_foul'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct'),
        case((func.sum(PlayerStat.fta) > 0, (func.sum(PlayerStat.ftm) * 100.0 / func.sum(PlayerStat.fta))), else_=0).label('ft_pct')
    ).join(PlayerStat, Player.id == PlayerStat.player_id)\
     .join(Game, PlayerStat.game_id == Game.id)\
     .filter(Game.season_id == view_sid)\
     .group_by(Player.id).all()
     
    # スタッツ分析
    player_fields = {
        'avg_pts': {'label': '得点'}, 'fg_pct': {'label': 'FG%'}, 'three_p_pct': {'label': '3P%'},
        'ft_pct': {'label': 'FT%'}, 'avg_reb': {'label': 'リバウンド'}, 'avg_ast': {'label': 'アシスト'},
        'avg_stl': {'label': 'スティール'}, 'avg_blk': {'label': 'ブロック'},
        'avg_turnover': {'label': 'TO', 'reverse': True}, 'avg_foul': {'label': 'FOUL', 'reverse': True},
    }
    analyzed_stats = analyze_stats(player_id, all_players_stats, 'player_id', player_fields, limit=10)
    
    # 対象選手の平均スタッツを取得
    target_avg_stats = next((p for p in all_players_stats if p.player_id == player_id), None)

    # --- ★修正箇所: データがない場合のダミーデータ作成 ---
    if target_avg_stats is None:
        class ZeroStats:
            avg_pts = 0.0; avg_reb = 0.0; avg_ast = 0.0
            avg_stl = 0.0; avg_blk = 0.0; avg_turnover = 0.0
            avg_foul = 0.0; fg_pct = 0.0; three_p_pct = 0.0
            ft_pct = 0.0
        target_avg_stats = ZeroStats()
    # --------------------------------------------------
    
    # 2. ゲームログ取得
    game_stats = db.session.query(
        PlayerStat, Game.game_date, Game.home_team_id, Game.away_team_id, 
        Team_Home.name.label('home_team_name'), Team_Away.name.label('away_team_name'),
        Game.home_score, Game.away_score
    ).join(Game, PlayerStat.game_id == Game.id)\
     .join(Team_Home, Game.home_team_id == Team_Home.id)\
     .join(Team_Away, Game.away_team_id == Team_Away.id)\
     .filter(PlayerStat.player_id == player_id, Game.season_id == view_sid)\
     .order_by(Game.game_date.desc()).all()

    # 3. 受賞歴 (Awards) の取得
    awards_query = db.session.query(VoteResult, VoteConfig, Season)\
        .join(VoteConfig, VoteResult.vote_config_id == VoteConfig.id)\
        .outerjoin(Season, VoteConfig.season_id == Season.id)\
        .filter(VoteResult.player_id == player_id, VoteConfig.is_published == True).order_by(VoteConfig.created_at.desc()).all()
    
    player_awards = []
    for res, conf, seas in awards_query:
        is_winner = False
        award_name = ""
        award_type = conf.vote_type
        if conf.vote_type == 'weekly':
            if res.rank == 1: is_winner = True; award_name = f"{conf.title} - {res.category}"
        elif conf.vote_type == 'monthly':
            if res.rank == 1: is_winner = True; award_name = f"{conf.title} - {res.category}"
        elif conf.vote_type == 'all_star':
            if res.rank == 1: is_winner = True; award_name = f"{seas.name if seas else ''} All-Star ({res.category})"
        elif conf.vote_type == 'awards':
            if 'All JPL' in res.category or res.rank == 1: is_winner = True; award_name = f"{seas.name if seas else ''} {res.category}"
        if is_winner:
            player_awards.append({'title': award_name, 'type': award_type, 'date': conf.created_at.strftime('%Y-%m-%d')})

    leaders = get_stats_leaders(view_sid) 
    stat_titles = {'平均得点': '得点王', '平均リバウンド': 'リバウンド王', '平均アシスト': 'アシスト王', '平均スティール': 'スティール王', '平均ブロック': 'ブロック王'}
    for key, leader_list in leaders.items():
        if leader_list and leader_list[0][2] == player_id:
            award_title = stat_titles.get(key, key)
            is_duplicate = any(a['title'].endswith(award_title) for a in player_awards)
            if not is_duplicate: player_awards.insert(0, {'title': f"Current {award_title}", 'type': 'stat_leader', 'date': 'Running'})
     
    return render_template('player_detail.html', player=player, stats=analyzed_stats, avg_stats=target_avg_stats, game_stats=game_stats, awards=player_awards)

@app.route('/game/<int:game_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_game(game_id):
    game = Game.query.get_or_404(game_id)
    
    if request.method == 'POST':
        game.youtube_url_home = request.form.get('youtube_url_home')
        game.youtube_url_away = request.form.get('youtube_url_away')
        
        # ★追加: リザルト画像のURLを保存
        # (game_edit.htmlの隠しフィールドから送られてくる)
        result_image_url = request.form.get('result_image_url')
        if result_image_url:
            game.result_image_url = result_image_url

        # 既存のスタッツをリセット
        PlayerStat.query.filter_by(game_id=game_id).delete()
        
        home_total_score = 0
        away_total_score = 0
        
        # ヘルパー関数: 空白なら0、それ以外は数値に変換
        def get_val(key):
            val = request.form.get(key)
            if not val or val.strip() == '':
                return 0
            try:
                return int(val)
            except ValueError:
                return 0

        # ホーム・アウェイ両方の選手をループして保存
        for team in [game.home_team, game.away_team]:
            for player in team.players:
                # PTSの入力欄が存在するかチェック（出場した選手のみ処理するため）
                if f'player_{player.id}_pts' in request.form:
                    stat = PlayerStat(game_id=game.id, player_id=player.id)
                    db.session.add(stat)
                    
                    stat.pts = get_val(f'player_{player.id}_pts')
                    stat.ast = get_val(f'player_{player.id}_ast')
                    stat.reb = get_val(f'player_{player.id}_reb')
                    stat.stl = get_val(f'player_{player.id}_stl')
                    stat.blk = get_val(f'player_{player.id}_blk')
                    stat.foul = get_val(f'player_{player.id}_foul')
                    stat.turnover = get_val(f'player_{player.id}_turnover')
                    stat.fgm = get_val(f'player_{player.id}_fgm')
                    stat.fga = get_val(f'player_{player.id}_fga')
                    stat.three_pm = get_val(f'player_{player.id}_three_pm')
                    stat.three_pa = get_val(f'player_{player.id}_three_pa')
                    stat.ftm = get_val(f'player_{player.id}_ftm')
                    stat.fta = get_val(f'player_{player.id}_fta')
                    
                    # チーム合計得点に加算
                    if team.id == game.home_team_id:
                        home_total_score += stat.pts
                    else:
                        away_total_score += stat.pts
        
        game.home_score = home_total_score
        game.away_score = away_total_score
        game.is_finished = True
        game.winner_id = None
        game.loser_id = None
        game.result_input_time = datetime.now()
        
        db.session.commit()
        flash('試合結果が更新されました。')
        return redirect(url_for('game_result', game_id=game.id))
    
    # GET時のデータ準備
    stats = {str(stat.player_id): {
        'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 
        'stl': stat.stl, 'blk': stat.blk, 'foul': stat.foul, 
        'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga, 
        'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 
        'ftm': stat.ftm, 'fta': stat.fta
    } for stat in PlayerStat.query.filter_by(game_id=game_id).all()}
    
    return render_template('game_edit.html', game=game, stats=stats)

@app.route('/game/<int:game_id>/result')
def game_result(game_id):
    game = Game.query.get_or_404(game_id)
    stats = {
        str(stat.player_id): {
            'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 'stl': stat.stl, 'blk': stat.blk,
            'foul': stat.foul, 'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga,
            'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 'ftm': stat.ftm, 'fta': stat.fta
        } for stat in PlayerStat.query.filter_by(game_id=game_id).all()
    }
    return render_template('game_result.html', game=game, stats=stats)

@app.route('/game/<int:game_id>/swap', methods=['POST'])
@login_required
@admin_required
def swap_teams(game_id):
    game = Game.query.get_or_404(game_id)
    original_home_id = game.home_team_id
    game.home_team_id = game.away_team_id; game.away_team_id = original_home_id
    if game.is_finished:
        original_home_score = game.home_score; game.home_score = game.away_score; game.away_score = original_home_score
        original_youtube_home = game.youtube_url_home; original_youtube_away = game.youtube_url_away 
        game.youtube_url_home = original_youtube_away; game.youtube_url_away = original_youtube_home
    try: db.session.commit(); flash(f'試合 (ID: {game.id}) のホームとアウェイを入れ替えました。')
    except Exception as e: db.session.rollback(); flash(f'入れ替え中にエラーが発生しました: {e}')
    return redirect(url_for('schedule'))

@app.route('/game/<int:game_id>/update_date', methods=['POST'])
@login_required
@admin_required
def update_game_date(game_id):
    game = Game.query.get_or_404(game_id)
    new_date = request.form.get('new_game_date'); new_time = request.form.get('new_game_time') 
    if new_date and new_time: 
        try:
            datetime.strptime(new_date, '%Y-%m-%d'); datetime.strptime(new_time, '%H:%M') 
            game.game_date = new_date; game.start_time = new_time 
            db.session.commit(); flash(f'試合 (ID: {game.id}) の日程を {new_date} {new_time} に変更しました。')
        except ValueError: flash('無効な日付または時間の形式です。')
    else: flash('新しい日付と時間の両方を指定してください。')
    return redirect(url_for('schedule'))

@app.route('/game/delete/<int:game_id>', methods=['POST'])
@login_required
@admin_required
def delete_game(game_id):
    if request.form.get('password') == 'delete':
        game_to_delete = Game.query.get_or_404(game_id)
        PlayerStat.query.filter_by(game_id=game_id).delete()
        db.session.delete(game_to_delete); db.session.commit()
        flash('試合日程を削除しました。')
    else: flash('パスワードが違います。削除はキャンセルされました。')
    return redirect(url_for('schedule'))

@app.route('/schedule/delete/all', methods=['POST'])
@login_required
@admin_required
def delete_all_schedules():
    if request.form.get('password') == 'delete':
        try:
            season = get_current_season()
            games = Game.query.filter_by(season_id=season.id).all()
            for g in games:
                PlayerStat.query.filter_by(game_id=g.id).delete()
                db.session.delete(g)
            db.session.commit()
            flash('現在のシーズン全日程と試合結果が削除されました。')
        except Exception as e: db.session.rollback(); flash(f'削除中にエラーが発生しました: {e}')
    else: flash('パスワードが違います。削除はキャンセルされました。')
    return redirect(url_for('schedule'))

# ★重要修正: 不戦勝ロジック
@app.route('/game/<int:game_id>/forfeit', methods=['POST'])
@login_required
@admin_required
def forfeit_game(game_id):
    game = Game.query.get_or_404(game_id)
    winning_team_id = request.form.get('winning_team_id', type=int)
    
    if winning_team_id == game.home_team_id:
        game.winner_id = game.home_team_id
        game.loser_id = game.away_team_id
    elif winning_team_id == game.away_team_id:
        game.winner_id = game.away_team_id
        game.loser_id = game.home_team_id
    else:
        flash('無効なチームが選択されました。')
        return redirect(url_for('edit_game', game_id=game_id))
    
    # 不戦試合としてマークし、スコアを0にする
    game.is_finished = True
    game.is_forfeit = True # ★これが必要
    game.home_score = 0
    game.away_score = 0
    
    # 既存のスタッツを消去
    PlayerStat.query.filter_by(game_id=game_id).delete()
    
    db.session.commit()
    flash('不戦勝として試合結果を記録しました。')
    return redirect(url_for('schedule'))

# --- 管理: チーム/選手操作 ---
@app.route('/admin/vote', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_vote_dashboard():
    season = get_current_season()
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'create':
            new_config = VoteConfig(
                season_id=season.id,
                title=request.form.get('title'),
                vote_type=request.form.get('vote_type'),
                description=request.form.get('description'),
                start_date=request.form.get('start_date'),
                end_date=request.form.get('end_date')
            )
            db.session.add(new_config)
            db.session.commit()
            flash('投票イベントを作成しました。')
        elif action == 'toggle_status':
            config = VoteConfig.query.get(request.form.get('config_id'))
            if config:
                config.is_open = not config.is_open
                db.session.commit()
                flash('ステータスを更新しました。')
        elif action == 'calculate_review':
            config_id = request.form.get('config_id')
            calculate_vote_results(config_id)
            return redirect(url_for('admin_vote_review', config_id=config_id))
        elif action == 'delete':
            config = VoteConfig.query.get(request.form.get('config_id'))
            if config:
                VoteResult.query.filter_by(vote_config_id=config.id).delete()
                Vote.query.filter_by(vote_config_id=config.id).delete()
                db.session.delete(config)
                db.session.commit()
                flash('削除しました。')
        elif action == 'hide_from_home':
            config = VoteConfig.query.get(request.form.get('config_id'))
            if config:
                config.show_on_home = False
                db.session.commit()
                flash('トップページから非表示にしました。（データは選手ページに残ります）')
        elif action == 'show_on_home':
            config = VoteConfig.query.get(request.form.get('config_id'))
            if config:
                config.show_on_home = True
                db.session.commit()
                flash('トップページに再表示しました。')
    configs = VoteConfig.query.filter_by(season_id=season.id).order_by(VoteConfig.created_at.desc()).all()
    votes_detail = {}
    for c in configs:
        votes = db.session.query(Vote, User).join(User).filter(Vote.vote_config_id == c.id).all()
        user_votes = defaultdict(list)
        for v, u in votes: user_votes[u.username].append(f"{v.category}: {v.player.name}")
        votes_detail[c.id] = dict(user_votes)
    return render_template('admin_vote.html', configs=configs, votes_detail=votes_detail)

@app.route('/admin/vote/review/<int:config_id>', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_vote_review(config_id):
    config = VoteConfig.query.get_or_404(config_id)
    if request.method == 'POST':
        results = VoteResult.query.filter_by(vote_config_id=config.id).all()
        for res in results:
            new_rank = request.form.get(f'rank_{res.id}')
            if new_rank: res.rank = int(new_rank)
        config.is_published = True; config.is_open = False; config.show_on_home = True
        db.session.commit(); flash('結果を公開しました。'); return redirect(url_for('index'))
    results = VoteResult.query.filter_by(vote_config_id=config.id).order_by(VoteResult.category, VoteResult.rank).all()
    grouped_results = defaultdict(list)
    for r in results: grouped_results[r.category].append(r)
    ties = {cat: (len([i.score for i in items]) != len(set([i.score for i in items]))) for cat, items in grouped_results.items()}
    return render_template('admin_vote_review.html', config=config, grouped_results=grouped_results, ties=ties)

@app.route('/vote/<int:config_id>', methods=['GET', 'POST'])
@login_required
def vote_page(config_id):
    config = VoteConfig.query.get_or_404(config_id)
    if not config.is_open and not current_user.is_admin: flash('この投票は現在受け付けていません。'); return redirect(url_for('index'))
    existing_vote = Vote.query.filter_by(vote_config_id=config_id, user_id=current_user.id).first()
    if existing_vote and request.method == 'GET': flash('すでにこのイベントには投票済みです。'); return redirect(url_for('index'))
    eligible_players_a = []; eligible_players_b = []; eligible_players = [] 
    if config.vote_type in ['weekly', 'monthly']:
        start_date = config.start_date; end_date = config.end_date
        if not start_date or not end_date:
            eligible_players_a = Player.query.join(Team).filter(Team.league == 'Aリーグ', Player.is_active==True).order_by(Player.name).all()
            eligible_players_b = Player.query.join(Team).filter(Team.league == 'Bリーグ', Player.is_active==True).order_by(Player.name).all()
        else:
            impact_score = (func.sum(PlayerStat.pts) + func.sum(PlayerStat.reb) + func.sum(PlayerStat.ast) + func.sum(PlayerStat.stl) + func.sum(PlayerStat.blk) - func.sum(PlayerStat.turnover) - (func.sum(PlayerStat.fga) - func.sum(PlayerStat.fgm)) - (func.sum(PlayerStat.fta) - func.sum(PlayerStat.ftm))) / func.count(PlayerStat.game_id)
            def get_top5_by_league(league_name):
                return db.session.query(Player).join(PlayerStat, Player.id == PlayerStat.player_id).join(Team, Player.team_id == Team.id).join(Game, PlayerStat.game_id == Game.id).filter(Game.game_date >= start_date, Game.game_date <= end_date, Team.league == league_name, Game.season_id == config.season_id, Player.is_active == True).group_by(Player.id).having(func.count(PlayerStat.game_id) >= 1).order_by(db.desc(impact_score)).limit(5).all()
            eligible_players_a = get_top5_by_league("Aリーグ"); eligible_players_b = get_top5_by_league("Bリーグ")
            if not eligible_players_a: eligible_players_a = Player.query.join(Team).filter(Team.league == 'Aリーグ', Player.is_active==True).all()
            if not eligible_players_b: eligible_players_b = Player.query.join(Team).filter(Team.league == 'Bリーグ', Player.is_active==True).all()
    elif config.vote_type == 'awards':
        teams = Team.query.filter_by(is_active=True).all(); max_games_played = 0
        for t in teams:
            count = Game.query.filter((Game.is_finished == True) & ((Game.home_team_id == t.id) | (Game.away_team_id == t.id)) & (Game.season_id == config.season_id)).count()
            if count > max_games_played: max_games_played = count
        limit_games = max_games_played * 0.7
        all_players = Player.query.join(Team).filter(Player.is_active==True).order_by(Team.id, Player.name).all()
        for p in all_players:
            p_games = PlayerStat.query.join(Game).filter(PlayerStat.player_id==p.id, Game.season_id==config.season_id).count()
            if max_games_played == 0 or p_games >= limit_games: eligible_players.append(p)
    elif config.vote_type == 'all_star':
        eligible_players = Player.query.join(Team).filter(Player.is_active==True).order_by(Team.id, Player.name).all()
    if request.method == 'POST':
        try:
            Vote.query.filter_by(vote_config_id=config_id, user_id=current_user.id).delete()
            if config.vote_type == 'weekly':
                pid_a = request.form.get('weekly_mvp_a'); pid_b = request.form.get('weekly_mvp_b')
                if pid_a: db.session.add(Vote(vote_config_id=config.id, user_id=current_user.id, player_id=pid_a, category="Weekly MVP A League"))
                if pid_b: db.session.add(Vote(vote_config_id=config.id, user_id=current_user.id, player_id=pid_b, category="Weekly MVP B League"))
            elif config.vote_type == 'monthly':
                pid_a = request.form.get('monthly_mvp_a'); pid_b = request.form.get('monthly_mvp_b')
                if pid_a: db.session.add(Vote(vote_config_id=config.id, user_id=current_user.id, player_id=pid_a, category="Monthly MVP A League"))
                if pid_b: db.session.add(Vote(vote_config_id=config.id, user_id=current_user.id, player_id=pid_b, category="Monthly MVP B League"))
            else:
                for key, value in request.form.items():
                    if value and value != "":
                        player_id = int(value); category = key; rank_point = 1
                        if config.vote_type == 'awards':
                            if '1st' in key: rank_point = 5
                            elif '2nd' in key: rank_point = 3
                            elif '3rd' in key: rank_point = 1
                            if 'all_jpl' in key: parts = key.split('_'); category = f"All JPL {parts[2]}" 
                            elif 'mvp' in key: category = 'MVP'
                            elif 'dpoy' in key: category = 'DPOY'
                        elif config.vote_type == 'all_star': category = key.replace('_', ' ')
                        db.session.add(Vote(vote_config_id=config.id, user_id=current_user.id, player_id=player_id, category=category, rank_value=rank_point))
            db.session.commit(); flash('投票を受け付けました！'); return redirect(url_for('index'))
        except Exception as e: db.session.rollback(); flash(f'エラーが発生しました: {e}'); return redirect(url_for('vote_page', config_id=config_id))
    return render_template('vote_form.html', config=config, eligible_players_a=eligible_players_a, eligible_players_b=eligible_players_b, players=eligible_players)

# --- 4. 集計コアロジック ---
def calculate_vote_results(config_id):
    config = VoteConfig.query.get(config_id)
    VoteResult.query.filter_by(vote_config_id=config_id).delete()
    votes = Vote.query.filter_by(vote_config_id=config_id).all()
    tally = defaultdict(lambda: defaultdict(int)); player_pos_votes = defaultdict(lambda: defaultdict(int))
    for v in votes:
        if config.vote_type in ['all_star', 'awards'] and ('All JPL' in v.category or 'League' in v.category):
            pos = v.category.split(' ')[-1]; player_pos_votes[v.player_id][pos] += v.rank_value; player_pos_votes[v.player_id]['total'] += v.rank_value
        else: tally[v.category][v.player_id] += v.rank_value
    if config.vote_type in ['all_star', 'awards']:
        for pid, pos_data in player_pos_votes.items():
            if 'total' in pos_data:
                total = pos_data.pop('total'); best_pos = max(pos_data, key=pos_data.get)
                if config.vote_type == 'all_star': p = Player.query.get(pid); cat = f"{p.team.league} {best_pos}"
                else: cat = f"All JPL {best_pos}"
                tally[cat][pid] = total
    for category, scores in tally.items():
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        for i, (pid, score) in enumerate(ranked):
            rank = i + 1; save_cat = category
            if config.vote_type == 'awards' and 'All JPL' in category:
                if rank == 1: save_cat += " 1st Team"
                elif rank == 2: save_cat += " 2nd Team"
                elif rank == 3: save_cat += " 3rd Team"
                else: continue 
            db.session.add(VoteResult(vote_config_id=config_id, category=save_cat, player_id=pid, score=score, rank=rank))
    db.session.commit()

# --- メインページ ---
@app.route('/')
def index():
    view_sid = get_view_season_id()
    overall_standings = calculate_standings(view_sid)
    league_a_standings = calculate_standings(view_sid, league_filter="Aリーグ")
    league_b_standings = calculate_standings(view_sid, league_filter="Bリーグ")
    stats_leaders = get_stats_leaders(view_sid)
    closest_game = Game.query.filter(Game.season_id == view_sid, Game.is_finished == False).order_by(Game.game_date.asc()).first()
    upcoming_games = Game.query.filter(Game.season_id == view_sid, Game.is_finished == False, Game.game_date == closest_game.game_date).order_by(Game.start_time.asc()).all() if closest_game else []
    news_items = News.query.order_by(News.created_at.desc()).limit(5).all()
    one_hour_ago = datetime.now() - timedelta(hours=1)
    latest_result_game = Game.query.filter(Game.season_id == view_sid, Game.is_finished == True, Game.result_input_time >= one_hour_ago).order_by(Game.result_input_time.desc()).first()
    
    all_candidates = MVPCandidate.query.all()
    weekly_candidates_a = [c for c in all_candidates if c.league_name == 'Aリーグ' and c.candidate_type == 'weekly']
    weekly_candidates_b = [c for c in all_candidates if c.league_name == 'Bリーグ' and c.candidate_type == 'weekly']
    monthly_candidates_a = [c for c in all_candidates if c.league_name == 'Aリーグ' and c.candidate_type == 'monthly']
    monthly_candidates_b = [c for c in all_candidates if c.league_name == 'Bリーグ' and c.candidate_type == 'monthly']
    
    setting = SystemSetting.query.get('show_mvp')
    show_mvp = True if setting and setting.value == 'true' else False
    all_teams = Team.query.order_by(Team.name).all()
    active_votes = VoteConfig.query.filter_by(season_id=view_sid, is_open=True).all()
    published_votes = VoteConfig.query.filter_by(season_id=view_sid, is_published=True, show_on_home=True).order_by(VoteConfig.created_at.desc()).limit(3).all()
    playoff_matches = PlayoffMatch.query.filter_by(season_id=view_sid).all()
    bracket_data = {'A': {1:[], 2:[], 3:[]}, 'B': {1:[], 2:[], 3:[]}, 'Final': []}
    r_map = {'1st Round': 1, 'Semi Final': 2, 'Conf Final': 3, 'Grand Final': 4}
    for m in playoff_matches:
        rn = r_map.get(m.round_name, 0); m.team1_obj = Team.query.get(m.team1_id) if m.team1_id else None; m.team2_obj = Team.query.get(m.team2_id) if m.team2_id else None
        if m.league == 'Final': bracket_data['Final'].append(m)
        elif m.league in bracket_data and rn in bracket_data[m.league]: bracket_data[m.league][rn].append(m)
    show_playoff = SystemSetting.query.get('show_playoff')
    show_playoff = True if show_playoff and show_playoff.value == 'true' else False

    # ★追加: 速報ティッカー情報の取得
    ticker_text_obj = SystemSetting.query.get('ticker_text')
    ticker_active_obj = SystemSetting.query.get('ticker_active')
    ticker_content = ticker_text_obj.value if ticker_text_obj else ""
    show_ticker = True if ticker_active_obj and ticker_active_obj.value == 'true' and ticker_content else False

    return render_template('index.html', overall_standings=overall_standings, league_a_standings=league_a_standings, league_b_standings=league_b_standings, leaders=stats_leaders, upcoming_games=upcoming_games, news_items=news_items, latest_result=latest_result_game, all_teams=all_teams, weekly_candidates_a=weekly_candidates_a, weekly_candidates_b=weekly_candidates_b, monthly_candidates_a=monthly_candidates_a, monthly_candidates_b=monthly_candidates_b, show_mvp=show_mvp, active_votes=active_votes, published_votes=published_votes, bracket=bracket_data, show_playoff=show_playoff, 
    show_ticker=show_ticker, ticker_content=ticker_content)

@app.route('/stats')
def stats_page():
    view_sid = get_view_season_id()
    # ★修正: チームスタッツも calculate_standings のロジックを使って正しい値を表示する
    team_stats = calculate_standings(view_sid)
    
    individual_stats = db.session.query(
        Player.id.label('player_id'), Player.name.label('player_name'), Team.id.label('team_id'), Team.name.label('team_name'),
        func.count(PlayerStat.game_id).label('games_played'), func.avg(PlayerStat.pts).label('avg_pts'),
        func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.reb).label('avg_reb'),
        func.avg(PlayerStat.stl).label('avg_stl'), func.avg(PlayerStat.blk).label('avg_blk'),
        func.avg(PlayerStat.foul).label('avg_foul'), func.avg(PlayerStat.turnover).label('avg_turnover'),
        func.avg(PlayerStat.fgm).label('avg_fgm'), func.avg(PlayerStat.fga).label('avg_fga'),
        func.avg(PlayerStat.three_pm).label('avg_three_pm'), func.avg(PlayerStat.three_pa).label('avg_three_pa'),
        func.avg(PlayerStat.ftm).label('avg_ftm'), func.avg(PlayerStat.fta).label('avg_fta'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct'),
        case((func.sum(PlayerStat.fta) > 0, (func.sum(PlayerStat.ftm) * 100.0 / func.sum(PlayerStat.fta))), else_=0).label('ft_pct')
    ).join(Player, PlayerStat.player_id == Player.id).join(Team, Player.team_id == Team.id)\
     .join(Game, PlayerStat.game_id == Game.id).filter(Game.season_id == view_sid)\
     .group_by(Player.id, Team.id, Team.name).all()
    return render_template('stats.html', team_stats=team_stats, individual_stats=individual_stats)

@app.route('/regulations')
def regulations(): return render_template('regulations.html')

@app.cli.command('init-db')
def init_db_command():
    db.create_all()
    print('Initialized the database.')

# --- ★追加: 選手比較機能 ---
@app.route('/compare', methods=['GET', 'POST'])
def compare_players():
    view_sid = get_view_season_id()
    # 全選手リスト（選択肢用）
    all_players = Player.query.join(Team).filter(Player.is_active==True).order_by(Team.id, Player.name).all()
    
    player1 = None
    player2 = None
    stats1 = None
    stats2 = None
    
    p1_id = request.args.get('p1', type=int)
    p2_id = request.args.get('p2', type=int)

    if request.method == 'POST':
        p1_id = request.form.get('player1', type=int)
        p2_id = request.form.get('player2', type=int)
        return redirect(url_for('compare_players', p1=p1_id, p2=p2_id))

    if p1_id:
        player1 = Player.query.get(p1_id)
        stats1 = _get_player_avg_stats(p1_id, view_sid)
    
    if p2_id:
        player2 = Player.query.get(p2_id)
        stats2 = _get_player_avg_stats(p2_id, view_sid)

    return render_template('compare.html', 
                           all_players=all_players,
                           p1=player1, s1=stats1,
                           p2=player2, s2=stats2)

def _get_player_avg_stats(player_id, season_id):
    """ 指定選手の平均スタッツを取得するヘルパー関数 """
    stats = db.session.query(
        func.avg(PlayerStat.pts).label('pts'),
        func.avg(PlayerStat.reb).label('reb'),
        func.avg(PlayerStat.ast).label('ast'),
        func.avg(PlayerStat.stl).label('stl'),
        func.avg(PlayerStat.blk).label('blk'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct')
    ).join(Game, PlayerStat.game_id == Game.id)\
     .filter(PlayerStat.player_id == player_id, Game.season_id == season_id).first()
    
    # Noneの場合は0を返す辞書を作成
    if not stats:
        return {'pts':0, 'reb':0, 'ast':0, 'stl':0, 'blk':0, 'fg_pct':0, 'three_p_pct':0}
    
    return {
        'pts': float(stats.pts or 0),
        'reb': float(stats.reb or 0),
        'ast': float(stats.ast or 0),
        'stl': float(stats.stl or 0),
        'blk': float(stats.blk or 0),
        'fg_pct': float(stats.fg_pct or 0),
        'three_p_pct': float(stats.three_p_pct or 0)
    }

# --- ★緊急用: DBカラム強制追加ルート (image_url用) ---
@app.route('/admin/fix_db_image')
@login_required
@admin_required
def fix_db_image_column():
    try:
        with db.engine.connect() as conn:
            trans = conn.begin()
            try:
                # playerテーブルに image_url カラムを追加
                conn.execute(text("ALTER TABLE player ADD COLUMN image_url VARCHAR(255)"))
                print("Added image_url column.")
                flash('Playerテーブルに image_url カラムを追加しました。')
            except Exception as e:
                print(f"image_url column might already exist: {e}")
                flash(f'カラム追加スキップ（既に存在する可能性があります）: {e}')
            
            trans.commit()
    except Exception as e:
        flash(f'DB操作エラー: {e}')
    
    return redirect(url_for('index'))


# --- ★追加: 試合パスワード変更機能 ---
@app.route('/game/<int:game_id>/update_password', methods=['POST'])
@login_required
@admin_required
def update_game_password(game_id):
    game = Game.query.get_or_404(game_id)
    new_password = request.form.get('new_password')
    
    if new_password:
        game.game_password = new_password
        db.session.commit()
        flash(f'試合ID {game.id} のパスワードを「{new_password}」に変更しました。')
    else:
        flash('パスワードが入力されていません。')
        
    # 元いたページ（日程ページ）に戻る
    return redirect(url_for('schedule'))

# --- ★追加: 選手画像アップロード機能 ---
@app.route('/player/<int:player_id>/upload_image', methods=['POST'])
def upload_player_image(player_id):
    # ログインチェック (ログインしていなければエラーメッセージを出してログイン画面へ)
    if not current_user.is_authenticated:
        flash('アップロードするにはログインが必要です\n運営から配布されている各チームアカウントでログインしてください')
        return redirect(url_for('login'))

    player = Player.query.get_or_404(player_id)
    
    if 'player_image' in request.files:
        file = request.files['player_image']
        if file and file.filename != '' and allowed_file(file.filename):
            try:
                # 古い画像があれば削除 (チームロゴと重複しないように注意が必要だが、基本は上書きでOK)
                if player.image_url and 'nba2k_jpl_cards' in player.image_url:
                    try:
                        public_id = "nba2k_jpl_cards/" + os.path.splitext(player.image_url.split('/')[-1])[0]
                        cloudinary.uploader.destroy(public_id)
                    except: pass

                # Cloudinaryへアップロード (幅500pxにリサイズして容量節約)
                upload_result = cloudinary.uploader.upload(
                    file, 
                    folder="nba2k_jpl_cards/players",
                    width=500, crop="limit"
                )
                player.image_url = upload_result.get('secure_url')
                db.session.commit()
                flash(f'選手「{player.name}」の画像を更新しました。')
            except Exception as e:
                flash(f'アップロードエラー: {e}')
        else:
            flash('ファイルが選択されていないか、対応していない形式です。')
    
    return redirect(url_for('player_detail', player_id=player_id))

@app.route('/api/analyze_stats', methods=['POST'])
@login_required
def analyze_stats_image():
    if 'image' not in request.files:
        return jsonify({'error': '画像がありません'}), 400
        
    file = request.files['image']
    if not file:
        return jsonify({'error': 'ファイルが無効です'}), 400

    # APIキー取得
    api_key = os.environ.get('GOOGLE_API_KEY')
    if not api_key:
        return jsonify({'error': 'APIキーが設定されていません'}), 500

    try:
        # ★追加: 先にCloudinaryへアップロード
        # (Cloudinaryの設定は環境変数から自動で読み込まれます)
        upload_result = cloudinary.uploader.upload(file)
        image_url = upload_result['secure_url']

        # ★重要: ファイルポインタを先頭に戻す
        # (一度アップロードで読み込んだため、戻さないとPillowで読み込めません)
        file.seek(0)

        # AI解析の準備
        genai.configure(api_key=api_key)
        # ユーザー推奨の最新モデル
        model = genai.GenerativeModel('gemini-2.5-flash') 
        img = Image.open(file)

        # AIへの命令（プロンプト）
        prompt_text = """
        タスク: このバスケットボールのボックススコア画像に含まれる【全ての選手】のスタッツを抽出してください。
        
        重要なルール:
        1. 画像内に表示されている【全ての行（選手）】を漏らさず抽出してください。省略は許されません。
        2. ホーム・アウェイなどチームが分かれている場合も、全ての選手を1つの "players" リストにまとめてください。
        3. 選手名は画像内の文字を可能な限り正確に読み取ってください。
        4. 以下のJSONフォーマットのみを出力してください（Markdownタグは不要）。
        
        {
            "players": [
                {
                    "name": "選手名",
                    "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0,
                    "foul": 0, "to": 0,
                    "fgm": 0, "fga": 0, 
                    "3pm": 0, "3pa": 0,
                    "ftm": 0, "fta": 0
                }
            ]
        }
        ※数値がない箇所は0。FGM/FGAなどは "5/10" のような表記から分割して数値化すること。
        """

        response = model.generate_content([prompt_text, img])
        
        # 余計な文字を削除してJSON化
        result_text = response.text.replace("```json", "").replace("```", "")
        data = json.loads(result_text)

        # ★追加: AIの結果JSONに「画像のURL」を追加して返す
        data['image_url'] = image_url
        
        return jsonify(data)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': f'解析エラー: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)