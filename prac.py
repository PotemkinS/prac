from datetime import datetime
from sqlalchemy.exc import IntegrityError
from flask import Flask, request, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
import random

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///mail_segments.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class UserSegments(db.Model):
    __tablename__ = 'user_segments'
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    segment_id = db.Column(db.Integer, db.ForeignKey('segments.id'), primary_key=True)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String, unique=True, nullable=False)
    last_name = db.Column(db.String, nullable=False)
    first_name = db.Column(db.String, nullable=False)
    middle_name = db.Column(db.String, nullable=True)
    birth_date = db.Column(db.Date, nullable=True)
    gender = db.Column(db.String, nullable=True)
    segments = db.relationship('Segments', secondary='user_segments', back_populates='users')


class Segments(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, unique=True, nullable=False)
    description = db.Column(db.Text, nullable=True)
    users = db.relationship('Users', secondary='user_segments', back_populates='segments')

@app.route('/')
def index():
    users = Users.query.all()
    segments = Segments.query.all()
    return render_template('index.html', users=users, segments=segments)

def validate_user_data(data):
    errors = []
    email = data.get("email")
    last_name = data.get("last_name")
    first_name = data.get("first_name")

    if not email or "@" not in email:
        errors.append("Некорректный email")
    if not last_name:
        errors.append("Фамилия обязательна")
    if not first_name:
        errors.append("Имя обязательно")

    return errors

def validate_segment_data(data):
    errors = []
    name = data.get("name")
    if not name:
        errors.append("Название сегмента обязательно")

    return errors

@app.route('/user/add', methods=['POST'])
def add_user():
    data = request.get_json()
    birth_date = None
    if data.get('birth_date'):
        try:
            birth_date = datetime.strptime(data['birth_date'], "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"ошибка": "Неверный формат даты. Ожидается YYYY-MM-DD."}), 400

    user = Users(
        email=data['email'],
        last_name=data['last_name'],
        first_name=data['first_name'],
        middle_name=data.get('middle_name'),
        birth_date=birth_date,
        gender=data.get('gender')
    )

    try:
        db.session.add(user)
        db.session.commit()
        return jsonify({"сообщение": "Пользователь успешно добавлен", "user_id": user.id}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({"ошибка": "Пользователь с таким email уже существует"}), 400

@app.route('/segment/add', methods=['POST'])
def add_segment():
    data = request.get_json()
    errors = validate_segment_data(data)
    if errors:
        return jsonify({"ошибки": errors}), 400

    existing = Segments.query.filter_by(name=data['name']).first()
    if existing:
        return jsonify({"ошибка": "Сегмент с таким именем уже существует"}), 400

    segment = Segments(name=data['name'], description=data.get('description'))
    try:
        db.session.add(segment)
        db.session.commit()
        return jsonify({"сообщение": "Сегмент успешно добавлен", "segment_id": segment.id}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({"ошибка": "Нарушение уникальности. Сегмент с таким именем уже существует."}), 400


# изменить существующий сегмент
@app.route('/segment/update/<int:id>', methods=['PUT'])
def update_segment(id):
    data = request.get_json()
    segment = Segments.query.get(id)
    if not segment:
        return jsonify({"ошибка": "Сегмент не найден"}), 404

    new_name = data.get('name')
    if not new_name:
        return jsonify({"ошибка": "Название сегмента обязательно"}), 400
    
    existing = Segments.query.filter(Segments.name == new_name, Segments.id != id).first()
    if existing:
        return jsonify({"ошибка": "Сегмент с таким именем уже существует"}), 400

    segment.name = new_name
    segment.description = data.get('description', segment.description)

    try:
        db.session.commit()
        return jsonify({"сообщение": "Сегмент успешно обновлен"}), 200
    except IntegrityError:
        db.session.rollback()
        return jsonify({"ошибка": "Ошибка обновления сегмента: нарушение уникальности имени"}), 400

@app.route('/segment/delete/<int:segment_id>', methods=['DELETE'])
def delete_segment(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404
    db.session.delete(segment)
    db.session.commit()
    return {"message": "Segment deleted"}

@app.route('/segment/add_users_by_ids/<int:segment_id>', methods=['POST'])
def add_segment_to_users_by_ids(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    user_ids = request.json.get('user_ids', [])
    if not isinstance(user_ids, list) or not user_ids:
        return {"error": "user_ids list required"}, 400

    users = Users.query.filter(Users.id.in_(user_ids)).all()
    added_count = 0
    for user in users:
        assoc = UserSegments.query.filter_by(user_id=user.id, segment_id=segment.id).first()
        if not assoc:
            assoc = UserSegments(user_id=user.id, segment_id=segment.id)
            db.session.add(assoc)
            added_count += 1
    db.session.commit()
    return {"message": f"Segment {segment_id} added to {added_count} users by IDs"}, 200

@app.route('/segment/add_users_by_percent/<int:segment_id>', methods=['POST'])
def add_segment_to_users_by_percent(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    percent = request.json.get('percent')
    if not isinstance(percent, (int, float)) or not (0.0 <= percent <= 100.0):
        return {"error": "percent must be between 0 and 100"}, 400

    all_users = Users.query.all()
    num_to_assign = int(len(all_users) * (percent / 100.0))
    selected_users = random.sample(all_users, num_to_assign) if num_to_assign > 0 else []

    added_count = 0
    for user in selected_users:
        assoc = UserSegments.query.filter_by(user_id=user.id, segment_id=segment.id).first()
        if not assoc:
            assoc = UserSegments(user_id=user.id, segment_id=segment.id)
            db.session.add(assoc)
            added_count += 1
    db.session.commit()
    return {"message": f"Segment {segment_id} added randomly to {added_count} users"}, 200

@app.route('/user/<int:user_id>/segments', methods=['GET'])
def get_segments_of_user(user_id):
    user = Users.query.get(user_id)
    if not user:
        return {"error": "User not found"}, 404

    segments = [{"id": seg.id, "name": seg.name, "description": seg.description} for seg in user.segments]
    return {"user_id": user_id, "segments": segments}

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)

#проверить работу можно по адресу http://84.252.133.40