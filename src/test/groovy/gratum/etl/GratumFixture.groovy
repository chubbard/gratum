package gratum.etl

class GratumFixture {
    public static final List<Map> _people = [
            [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male', comment: """
I had the single cheese burger.  It was juicy and well seasoned.  The fries 
were on the soggy side and I had to wait for a while to get my milkshake.
"""],
            [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female', comment: """
I had the chicken salad.  It was delicious.  I would like more raisins next time.
"""],
            [id: 3, name: 'Diana Rogers', age: 34, gender: 'female', comment: """
I had to wait a very long time for my cheeseburger, and when it came it didn't have
cheese.  I had to send it back, but they got it right the second time.  The burger
was good, and the fries were crispy and warm.
"""],
            [id: 4, name: 'Jack Lowland', age: 25, gender: 'male', comment: """
I loved my burger and milkshake.
"""],
            [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female', comment: """
I had the chili dog and the onion rings, but I wish you had tater tots.
"""]
    ]

    public static final Collection<Map> _hobbies = [
            [id:1, hobby: 'Stamp Collecting'],
            [id:1, hobby: 'Bird Watching'],
            [id:2, hobby: 'Biking'],
            [id:2, hobby: 'Tennis'],
            [id:3, hobby: 'Archeology'],
            [id:3, hobby: 'Treasure Hunting'],
            [id:4, hobby: 'Crossfit'],
            [id:4, hobby: 'Obstacle Races']
    ]

    public static Collection<Map<String,Object>> getHobbies() {
        return _hobbies.collect() { (Map<String,Object>)it.clone() }
    }

    public static Collection<Map<String,Object>> getPeople() {
        return _people.collect() { (Map<String,Object>)it.clone() }
    }

}
