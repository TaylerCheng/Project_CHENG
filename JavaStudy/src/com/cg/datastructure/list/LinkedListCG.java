package com.cg.datastructure.list;


public class LinkedListCG<E> implements ListCG<E> {
	private int size;
	private LinkedListCG next;
	public E element;

	public void add(E e) {
		LinkedListCG head = this;
		while (head.next!= null) {
			head = head.next;
		}
		LinkedListCG tmp = new LinkedListCG();
		head.next = tmp;
		tmp.element = e;
		
		size++;
	}
	
	private static class Node<E> {
        E item;
        Node<E> next;
        Node<E> prev;

        Node(Node<E> prev, E element, Node<E> next) {
            this.item = element;
            this.next = next;
            this.prev = prev;
        }
    }
	
	
	public void remove(E e){
		LinkedListCG head = this;
		while (head.next!= null) {
			if (head.next.element.equals(e)) {
				head.next= head.next.next;
				return;
			}
			head = head.next;
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		LinkedListCG head = this;
		while (head.next!= null) {
			head=head.next;
			sb.append(head.element.toString() + ", ");
		}
		return sb.append("]").toString();

	}

	@Override
	public E get(int index) {
		return null;
	}
}
