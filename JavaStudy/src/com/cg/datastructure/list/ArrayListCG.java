package com.cg.datastructure.list;

import java.util.Arrays;

public class ArrayListCG<E> implements ListCG<E>{
	private int size;
	private int volume;
	private Object[] elementData;
	private final static int DEFAULT_CAPACITY = 10; 

	public ArrayListCG() {
		volume = DEFAULT_CAPACITY;
		elementData = new Object[DEFAULT_CAPACITY];
	}

	public ArrayListCG(int initsize) {
		volume = initsize;
		elementData = new Object[initsize];
	}

	public E get(int index) {
		if (index < 0 || index > size) {
			return null;
		}
		return (E)elementData[index];
	}

	public void add(E o) {
		if (size == volume) {
			elementData = Arrays.copyOf(elementData, volume * 2);
			volume *= 2;
		}
		elementData[size] = o;
		size++;
	}

	public void remove(int index) {
		if (index > size) {
			return;
		}
		for (int i = index; i < size; i++) {
			elementData[i] = elementData[i + 1];
		}
		size--;
	}

//	public void remove(Object o) {
//		for (int i = 0; i < size; i++) {
//
//		}
//		size--;
//	}
    public boolean isEmpty() {
        return size == 0;
    }
	
    public int size() {
        return size;
    }
    
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (int i = 0; i < size; i++) {
			sb.append(elementData[i].toString() + ", ");
		}
		return sb.append(']').toString();

	}
}
