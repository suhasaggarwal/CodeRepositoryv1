package com.spidio.util;


class Element implements Comparable<Element> {

    int index, value;

    Element(int index, int value){
        this.index = index;
        this.value = value;
    }

    public int compareTo(Element e) {
        return this.value - e.value;
    }
}