package gratum.etl

interface Openable {

    public <T> T asType(Class<T> clazz);

}