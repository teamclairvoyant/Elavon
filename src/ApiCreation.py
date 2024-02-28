from flask import Flask, request, jsonify
import hashlib

app = Flask(__name__)


@app.route('/calculate_hash', methods=['POST'])
def calculate_hash():
    try:
        # Get data from the request
        data = request.json.get('data_batch')
        # Get columns to hash from the request
        columns_to_hash = request.json.get('columns_to_hash')

        if not columns_to_hash:
            raise ValueError("No columns specified for hashing")

        # Calculate hash values for each column
        for d in data:
            for column in columns_to_hash:
                if column in d:
                    d[column] = hashlib.sha256(d[column].encode()).hexdigest()

        # Prepare response
        response = {'hash_value': data}
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
