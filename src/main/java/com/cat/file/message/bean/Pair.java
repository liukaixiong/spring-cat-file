package com.cat.file.message.bean;

public class Pair<K, V> implements Tuple {
    private volatile K m_key;
    private volatile V m_value;

    public Pair() {
    }

    public Pair(K key, V value) {
        this.m_key = key;
        this.m_value = value;
    }

    public static <K, V> Pair<K, V> from(K key, V value) {
        return new Pair(key, value);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Pair) {
            Pair<Object, Object> o = (Pair) obj;
            if (this.m_key == null) {
                if (o.m_key != null) {
                    return false;
                }
            } else if (!this.m_key.equals(o.m_key)) {
                return false;
            }

            if (this.m_value == null) {
                if (o.m_value != null) {
                    return false;
                }
            } else if (!this.m_value.equals(o.m_value)) {
                return false;
            }

            return true;
        } else {
            return false;
        }
    }

    public <T> T get(int index) {
        switch (index) {
            case 0:
                return (T) this.m_key;
            case 1:
                return (T) this.m_value;
            default:
                throw new IndexOutOfBoundsException(String.format("Index from 0 to %s, but was %s!", this.size(), index));
        }
    }

    public K getKey() {
        return this.m_key;
    }

    public V getValue() {
        return this.m_value;
    }

    public int hashCode() {
        int hash = 0;
        hash = hash * 31 + (this.m_key == null ? 0 : this.m_key.hashCode());
        hash = hash * 31 + (this.m_value == null ? 0 : this.m_value.hashCode());
        return hash;
    }

    public void setKey(K key) {
        this.m_key = key;
    }

    public void setValue(V value) {
        this.m_value = value;
    }

    public int size() {
        return 2;
    }

    public String toString() {
        return String.format("Pair[key=%s, value=%s]", this.m_key, this.m_value);
    }
}
