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
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, case, or_
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from functools import wraps
from collections import defaultdict, deque
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from itertools import product, combinations

# --- 1. アプリケーションとデータベースの初期設定 ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY') or 'dev_key_sample'
basedir = os.path.abspath(os.path.dirname(__file__))

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

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    logo_image = db.Column(db.String(255), nullable=True)
    league = db.Column(db.String(50), nullable=True)
    players = db.relationship('Player', backref='team', lazy=True)

class Player(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)

class Game(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_date = db.Column(db.String(50))
    start_time = db.Column(db.String(20), nullable=True)
    game_password = db.Column(db.String(50), nullable=True)
    home_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    away_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    home_score = db.Column(db.Integer, default=0)
    away_score = db.Column(db.Integer, default=0)
    is_finished = db.Column(db.Boolean, default=False)
    youtube_url_home = db.Column(db.String(200), nullable=True)
    youtube_url_away = db.Column(db.String(200), nullable=True)
    winner_id = db.Column(db.Integer, nullable=True)
    loser_id = db.Column(db.Integer, nullable=True)
    result_input_time = db.Column(db.DateTime, nullable=True) 
    home_team = db.relationship('Team', foreign_keys=[home_team_id])
    away_team = db.relationship('Team', foreign_keys=[away_team_id])

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

class News(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow) 
    def __repr__(self): return f'<News {self.title}>'

class MVPCandidate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    score = db.Column(db.Float, default=0.0)
    avg_pts = db.Column(db.Float, default=0.0); avg_reb = db.Column(db.Float, default=0.0)
    avg_ast = db.Column(db.Float, default=0.0); avg_stl = db.Column(db.Float, default=0.0)
    avg_blk = db.Column(db.Float, default=0.0)
    league_name = db.Column(db.String(50))
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
            flash("この操作には管理者権限が必要です。"); return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename): return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif'}
def generate_password(length=4): return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def calculate_standings(league_filter=None):
    if league_filter: teams = Team.query.filter_by(league=league_filter).all()
    else: teams = Team.query.all()
    standings = []
    for team in teams:
        wins, losses, points_for, points_against, stats_games_played = 0, 0, 0, 0, 0
        home_games = Game.query.filter_by(home_team_id=team.id, is_finished=True).all()
        for game in home_games:
            if game.winner_id is None:
                points_for += game.home_score; points_against += game.away_score; stats_games_played += 1
            if game.winner_id == team.id: wins += 1
            elif game.loser_id == team.id: losses += 1
            elif game.home_score > game.away_score: wins += 1
            elif game.home_score < game.away_score: losses += 1
        away_games = Game.query.filter_by(away_team_id=team.id, is_finished=True).all()
        for game in away_games:
            if game.winner_id is None:
                points_for += game.away_score; points_against += game.home_score; stats_games_played += 1
            if game.winner_id == team.id: wins += 1
            elif game.loser_id == team.id: losses += 1
            elif game.away_score > game.home_score: wins += 1
            elif game.away_score < game.home_score: losses += 1
        points = (wins * 2) + (losses * 1)
        standings.append({
            'team': team, 'team_name': team.name, 'league': team.league, 'wins': wins, 'losses': losses, 'points': points,
            'avg_pf': round(points_for / stats_games_played, 1) if stats_games_played > 0 else 0,
            'avg_pa': round(points_against / stats_games_played, 1) if stats_games_played > 0 else 0,
            'diff': points_for - points_against, 'stats_games_played': stats_games_played
        })
    standings.sort(key=lambda x: (x['points'], x['diff']), reverse=True)
    return standings

def get_stats_leaders():
    leaders = {}
    stat_fields = {'pts': '平均得点', 'ast': '平均アシスト', 'reb': '平均リバウンド', 'stl': '平均スティール', 'blk': '平均ブロック'}
    for field_key, field_name in stat_fields.items():
        avg_stat = func.avg(getattr(PlayerStat, field_key)).label('avg_value')
        query_result = db.session.query(Player.name, avg_stat, Player.id).join(PlayerStat, PlayerStat.player_id == Player.id).join(Game, PlayerStat.game_id == Game.id).filter(Game.is_finished == True).group_by(Player.id).order_by(db.desc('avg_value')).limit(5).all()
        leaders[field_name] = query_result
    return leaders

def calculate_team_stats():
    team_stats_list = []
    standings_info = calculate_standings()
    shooting_stats_query = db.session.query(
        Player.team_id, func.sum(PlayerStat.pts).label('total_pts'),
        func.sum(PlayerStat.ast).label('total_ast'), func.sum(PlayerStat.reb).label('total_reb'),
        func.sum(PlayerStat.stl).label('total_stl'), func.sum(PlayerStat.blk).label('total_blk'),
        func.sum(PlayerStat.foul).label('total_foul'), func.sum(PlayerStat.turnover).label('total_turnover'),
        func.sum(PlayerStat.fgm).label('total_fgm'), func.sum(PlayerStat.fga).label('total_fga'),
        func.sum(PlayerStat.three_pm).label('total_3pm'), func.sum(PlayerStat.three_pa).label('total_3pa'),
        func.sum(PlayerStat.ftm).label('total_ftm'), func.sum(PlayerStat.fta).label('total_fta')
    ).join(Player, PlayerStat.player_id == Player.id).join(Game, PlayerStat.game_id == Game.id).filter(Game.is_finished == True).group_by(Player.team_id).all()
    shooting_map = {s.team_id: s for s in shooting_stats_query}
    for team_standings in standings_info:
        team_obj = team_standings.get('team')
        if not team_obj: continue
        stats_games_played = team_standings.get('stats_games_played', 0)
        team_shooting = shooting_map.get(team_obj.id)
        stats_dict = team_standings.copy()
        if stats_games_played > 0 and team_shooting:
            stats_dict.update({
                'avg_ast': (team_shooting.total_ast or 0) / stats_games_played, 'avg_reb': (team_shooting.total_reb or 0) / stats_games_played,
                'avg_stl': (team_shooting.total_stl or 0) / stats_games_played, 'avg_blk': (team_shooting.total_blk or 0) / stats_games_played,
                'avg_foul': (team_shooting.total_foul or 0) / stats_games_played, 'avg_turnover': (team_shooting.total_turnover or 0) / stats_games_played,
                'avg_fgm': (team_shooting.total_fgm or 0) / stats_games_played, 'avg_fga': (team_shooting.total_fga or 0) / stats_games_played,
                'avg_three_pm': (team_shooting.total_3pm or 0) / stats_games_played, 'avg_three_pa': (team_shooting.total_3pa or 0) / stats_games_played,
                'avg_ftm': (team_shooting.total_ftm or 0) / stats_games_played, 'avg_fta': (team_shooting.total_fta or 0) / stats_games_played,
                'fg_pct': ((team_shooting.total_fgm or 0) / team_shooting.total_fga * 100) if (team_shooting.total_fga or 0) > 0 else 0,
                'three_p_pct': ((team_shooting.total_3pm or 0) / team_shooting.total_3pa * 100) if (team_shooting.total_3pa or 0) > 0 else 0,
                'ft_pct': ((team_shooting.total_ftm or 0) / team_shooting.total_fta * 100) if (team_shooting.total_fta or 0) > 0 else 0,
            })
        else:
            stats_dict.update({'avg_ast':0, 'avg_reb':0, 'avg_stl':0, 'avg_blk':0, 'avg_foul':0, 'avg_turnover':0, 'fg_pct':0, 'three_p_pct':0, 'ft_pct':0})
        team_stats_list.append(stats_dict)
    return team_stats_list

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

# ★★★ 修正版 analyze_stats: 引数 limit を追加 ★★★
def analyze_stats(target_id, all_data, id_key, fields_config, limit=5):
    result = {}
    
    # データをリスト化 (DictとRowオブジェクトの差異を吸収)
    data_list = []
    for item in all_data:
        entry = {}
        if isinstance(item, dict):
            current_id = item.get('team').id if 'team' in item else item.get(id_key)
            for field in fields_config.keys():
                entry[field] = item.get(field, 0) or 0
        else:
            current_id = getattr(item, id_key)
            for field in fields_config.keys():
                entry[field] = getattr(item, field, 0) or 0
        entry['id'] = current_id
        data_list.append(entry)

    for field, config in fields_config.items():
        values = [d[field] for d in data_list]
        avg_val = sum(values) / len(values) if values else 0
        target_val = next((d[field] for d in data_list if d['id'] == target_id), 0)
        
        reverse_sort = not config.get('reverse', False)
        sorted_values = sorted(values, reverse=reverse_sort)
        
        try:
            rank = sorted_values.index(target_val) + 1
        except ValueError:
            rank = len(values)

        # ★★★ 修正箇所: 引数 limit で判定し、クラス名を stat-top に統一 ★★★
        if rank <= limit:
            color_class = 'stat-top' # 赤
        elif (not config.get('reverse', False) and target_val >= avg_val) or \
             (config.get('reverse', False) and target_val <= avg_val):
            color_class = 'stat-good' # 黄
        else:
            color_class = 'stat-avg' # グレー

        result[field] = {
            'value': target_val, 'rank': rank, 'avg': avg_val,
            'color_class': color_class, 'label': config['label']
        }
    return result

# =========================================================
# 5. ルーティング
# =========================================================

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
        if auth_password != 'JPL':
            flash('認証パスワードが違います。'); return redirect(url_for('register'))
        username = request.form['username']
        if User.query.filter_by(username=username).first():
            flash("そのユーザー名は既に使用されています。"); return redirect(url_for('register'))
        role = 'admin' if User.query.count() == 0 else 'user'
        new_user = User(username=username, role=role)
        new_user.set_password(request.form['password'])
        db.session.add(new_user); db.session.commit()
        flash(f"ユーザー登録が完了しました。"); return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/mvp_selector', methods=['GET', 'POST'])
@login_required
@admin_required
def mvp_selector():
    top_players_a = []; top_players_b = []; start_date = None; end_date = None
    setting = SystemSetting.query.get('show_mvp')
    is_mvp_visible = True if setting and setting.value == 'true' else False

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'calculate' or action == 'publish':
            start_date_str = request.form.get('start_date'); end_date_str = request.form.get('end_date')
            if start_date_str and end_date_str:
                start_date = start_date_str; end_date = end_date_str
                # 簡易クエリ
                impact_score = (func.avg(PlayerStat.pts) + func.avg(PlayerStat.reb) + func.avg(PlayerStat.ast) + func.avg(PlayerStat.stl) + func.avg(PlayerStat.blk) - func.avg(PlayerStat.turnover) - (func.avg(PlayerStat.fga) - func.avg(PlayerStat.fgm)) - (func.avg(PlayerStat.fta) - func.avg(PlayerStat.ftm)))
                
                if action == 'calculate':
                    def get_top_players(league_name):
                        query = db.session.query(
                            Player, Team, func.count(PlayerStat.game_id).label('games_played'), impact_score.label('score'),
                            func.avg(PlayerStat.pts).label('avg_pts'), func.avg(PlayerStat.reb).label('avg_reb'),
                            func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.stl).label('avg_stl'),
                            func.avg(PlayerStat.blk).label('avg_blk')
                        )
                        query = query.join(PlayerStat, Player.id == PlayerStat.player_id).join(Team, Player.team_id == Team.id).join(Game, PlayerStat.game_id == Game.id)
                        query = query.filter(Game.game_date >= start_date, Game.game_date <= end_date, Team.league == league_name)
                        query = query.group_by(Player.id, Team.id).having(func.count(PlayerStat.game_id) >= 1).order_by(db.desc('score')).limit(5)
                        return query.all()
                    top_players_a = get_top_players("Aリーグ"); top_players_b = get_top_players("Bリーグ")
                    if not top_players_a and not top_players_b: flash('指定期間にデータがありません。')

                elif action == 'publish':
                    db.session.query(MVPCandidate).delete()
                    def save_top_players(league_name):
                        query = db.session.query(
                            Player.id, impact_score.label('score'),
                            func.avg(PlayerStat.pts).label('avg_pts'), func.avg(PlayerStat.reb).label('avg_reb'),
                            func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.stl).label('avg_stl'),
                            func.avg(PlayerStat.blk).label('avg_blk')
                        )
                        query = query.join(PlayerStat, Player.id == PlayerStat.player_id).join(Team, Player.team_id == Team.id).join(Game, PlayerStat.game_id == Game.id)
                        query = query.filter(Game.game_date >= start_date_str, Game.game_date <= end_date_str, Team.league == league_name)
                        query = query.group_by(Player.id, Team.id).having(func.count(PlayerStat.game_id) >= 1).order_by(db.desc('score')).limit(5)
                        results = query.all()
                        for r in results:
                            candidate = MVPCandidate(
                                player_id=r[0], score=r[1], avg_pts=r[2], avg_reb=r[3], 
                                avg_ast=r[4], avg_stl=r[5], avg_blk=r[6], league_name=league_name
                            )
                            db.session.add(candidate)
                    save_top_players("Aリーグ"); save_top_players("Bリーグ")
                    setting = SystemSetting.query.get('show_mvp')
                    if not setting: setting = SystemSetting(key='show_mvp', value='true'); db.session.add(setting)
                    else: setting.value = 'true'
                    db.session.commit(); flash('週間MVPをトップページに公開しました！'); return redirect(url_for('index'))

        elif action == 'toggle_visibility':
            current_val = request.form.get('current_visibility'); new_val = 'false' if current_val == 'true' else 'true'
            setting = SystemSetting.query.get('show_mvp')
            if not setting: setting = SystemSetting(key='show_mvp', value=new_val); db.session.add(setting)
            else: setting.value = new_val
            db.session.commit(); flash(f"表示を {'ON' if new_val=='true' else 'OFF'} にしました。"); return redirect(url_for('mvp_selector'))

    return render_template('mvp_selector.html', top_players_a=top_players_a, top_players_b=top_players_b, start_date=start_date, end_date=end_date, is_mvp_visible=is_mvp_visible)

@app.route('/team/<int:team_id>')
def team_detail(team_id):
    team = Team.query.get_or_404(team_id)
    all_team_stats_data = calculate_team_stats() 
    target_team_stats = next((item for item in all_team_stats_data if item['team'].id == team_id), {
        'wins': 0, 'losses': 0, 'points': 0, 'diff': 0, 'avg_pf': 0, 'avg_pa': 0, 'avg_reb': 0, 'avg_ast': 0, 'avg_stl': 0, 'avg_blk': 0, 'avg_turnover': 0, 'avg_foul': 0, 'fg_pct': 0, 'three_p_pct': 0, 'ft_pct': 0
    })
    
    # ★ チームは limit=5 ★
    team_fields = {
        'avg_pf': {'label': '平均得点'}, 'avg_pa': {'label': '平均失点', 'reverse': True}, 'diff': {'label': '得失点差'},
        'fg_pct': {'label': 'FG%'}, 'three_p_pct': {'label': '3P%'}, 'ft_pct': {'label': 'FT%'},
        'avg_reb': {'label': 'リバウンド'}, 'avg_ast': {'label': 'アシスト'}, 'avg_stl': {'label': 'スティール'},
        'avg_blk': {'label': 'ブロック'}, 'avg_turnover': {'label': 'ターンオーバー', 'reverse': True}, 'avg_foul': {'label': 'ファウル', 'reverse': True},
    }
    analyzed_stats = analyze_stats(team_id, all_team_stats_data, 'none', team_fields, limit=5)

    player_stats_list = db.session.query(
        Player, func.count(PlayerStat.game_id).label('games_played'),
        func.avg(PlayerStat.pts).label('avg_pts'), func.avg(PlayerStat.reb).label('avg_reb'),
        func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.stl).label('avg_stl'),
        func.avg(PlayerStat.blk).label('avg_blk'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct'),
        case((func.sum(PlayerStat.fta) > 0, (func.sum(PlayerStat.ftm) * 100.0 / func.sum(PlayerStat.fta))), else_=0).label('ft_pct')
    ).outerjoin(PlayerStat, Player.id == PlayerStat.player_id).filter(Player.team_id == team_id).group_by(Player.id).order_by(Player.name.asc()).all()

    team_games = Game.query.filter(or_(Game.home_team_id == team_id, Game.away_team_id == team_id)).order_by(Game.game_date.asc(), Game.start_time.asc()).all()
    players = Player.query.filter_by(team_id=team_id).all()

    return render_template('team_detail.html', team=team, players=players, player_stats_list=player_stats_list, team_games=team_games, team_stats=target_team_stats, stats=analyzed_stats)

@app.route('/player/<int:player_id>')
def player_detail(player_id):
    player = Player.query.get_or_404(player_id)
    all_players_stats = db.session.query(
        Player.id.label('player_id'), func.count(PlayerStat.game_id).label('games_played'),
        func.avg(PlayerStat.pts).label('avg_pts'), func.avg(PlayerStat.reb).label('avg_reb'),
        func.avg(PlayerStat.ast).label('avg_ast'), func.avg(PlayerStat.stl).label('avg_stl'),
        func.avg(PlayerStat.blk).label('avg_blk'), func.avg(PlayerStat.turnover).label('avg_turnover'),
        func.avg(PlayerStat.foul).label('avg_foul'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct'),
        case((func.sum(PlayerStat.fta) > 0, (func.sum(PlayerStat.ftm) * 100.0 / func.sum(PlayerStat.fta))), else_=0).label('ft_pct')
    ).join(PlayerStat, Player.id == PlayerStat.player_id).group_by(Player.id).all()

    # ★ 選手は limit=10 ★
    player_fields = {
        'avg_pts': {'label': '得点'}, 'fg_pct': {'label': 'FG%'}, 'three_p_pct': {'label': '3P%'},
        'ft_pct': {'label': 'FT%'}, 'avg_reb': {'label': 'リバウンド'}, 'avg_ast': {'label': 'アシスト'},
        'avg_stl': {'label': 'スティール'}, 'avg_blk': {'label': 'ブロック'},
        'avg_turnover': {'label': 'TO', 'reverse': True}, 'avg_foul': {'label': 'FOUL', 'reverse': True},
    }
    analyzed_stats = analyze_stats(player_id, all_players_stats, 'player_id', player_fields, limit=10)
    target_avg_stats = next((p for p in all_players_stats if p.player_id == player_id), None)
    
    game_stats = db.session.query(
        PlayerStat, Game.game_date, Game.home_team_id, Game.away_team_id, 
        Team_Home.name.label('home_team_name'), Team_Away.name.label('away_team_name'),
        Game.home_score, Game.away_score
    ).join(Game, PlayerStat.game_id == Game.id).join(Team_Home, Game.home_team_id == Team_Home.id).join(Team_Away, Game.away_team_id == Team_Away.id).filter(PlayerStat.player_id == player_id).order_by(Game.game_date.desc()).all()
    
    return render_template('player_detail.html', player=player, stats=analyzed_stats, avg_stats=target_avg_stats, game_stats=game_stats)

@app.route('/stats')
def stats_page():
    team_stats = calculate_team_stats()
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
    ).join(Player, PlayerStat.player_id == Player.id).join(Team, Player.team_id == Team.id).group_by(Player.id, Team.id, Team.name).all()
    return render_template('stats.html', team_stats=team_stats, individual_stats=individual_stats)

@app.route('/regulations')
def regulations(): return render_template('regulations.html')

@app.route('/')
def index():
    overall_standings = calculate_standings()
    league_a_standings = calculate_standings(league_filter="Aリーグ")
    league_b_standings = calculate_standings(league_filter="Bリーグ")
    stats_leaders = get_stats_leaders()
    
    closest_game = Game.query.filter(Game.is_finished == False).order_by(Game.game_date.asc()).first()
    upcoming_games = Game.query.filter(Game.is_finished == False, Game.game_date == closest_game.game_date).order_by(Game.start_time.asc()).all() if closest_game else []
    news_items = News.query.order_by(News.created_at.desc()).limit(5).all()
    
    one_hour_ago = datetime.now() - timedelta(hours=1)
    latest_result_game = Game.query.filter(Game.is_finished == True, Game.result_input_time >= one_hour_ago).order_by(Game.result_input_time.desc()).first()
    mvp_candidates = MVPCandidate.query.all()
    top_players_a = [c for c in mvp_candidates if c.league_name == 'Aリーグ']
    top_players_b = [c for c in mvp_candidates if c.league_name == 'Bリーグ']
    
    setting = SystemSetting.query.get('show_mvp')
    show_mvp = True if setting and setting.value == 'true' else False
    all_teams = Team.query.order_by(Team.name).all()
    
    return render_template('index.html', overall_standings=overall_standings, league_a_standings=league_a_standings, league_b_standings=league_b_standings, leaders=stats_leaders, upcoming_games=upcoming_games, news_items=news_items, latest_result=latest_result_game, all_teams=all_teams, top_players_a=top_players_a, top_players_b=top_players_b, show_mvp=show_mvp)

# その他ルート (edit_news, add_schedule, auto_schedule 等) は元のままです
# ... (省略: 上記以外のルートは既存のまま) ...

@app.cli.command('init-db')
def init_db_command():
    db.create_all()
    print('Initialized the database.')

if __name__ == '__main__':
    app.run(debug=True)