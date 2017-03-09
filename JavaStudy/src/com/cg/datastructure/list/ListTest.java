package com.cg.datastructure.list;

import java.util.Random;

public class ListTest {

	public static void main(String[] args) {
		// testArrayList();
		// testLinkedList();
		String str = "abc";
		System.out.println(str.hashCode());
		str = "abc";
		System.out.println(str.hashCode());
	}

	private static ListCG getList() {
		Random random = new Random();
		if (random.hashCode() % 2 == 1) {
			return new ArrayListCG();
		} else {
			return new LinkedListCG();
		}

	}

	private static void testLinkedList() {
		// LinkedList<String> list = new LinkedList();
		// list.add("a");
		// list.add("b");
		// list.add("c");
		// list.add("d");
		// list.remove("c");
		// System.out.println(list);

		// LinkedListCG<String> newlist = new LinkedListCG();
		// newlist.add("a");
		// newlist.add("b");
		// newlist.add("c");
		// newlist.add("d");
		// newlist.add("e");
		// newlist.remove("c");
		// System.out.println(newlist);

	}

	private static void testArrayList() {
		// ArrayList<String> numbers = new ArrayList();
		// numbers.add("a");
		// numbers.add("b");
		// numbers.add("c");
		// numbers.add("d");
		// numbers.remove(1);
		// System.out.println(numbers.toString());

		ArrayListCG<String> numbersCG = new ArrayListCG();
		numbersCG.add("a");
		numbersCG.add("b");
		numbersCG.add("c");
		numbersCG.add("d");
		numbersCG.add("e");
		numbersCG.add("f");
		numbersCG.add("g");
		numbersCG.add("h");
		numbersCG.add("i");
		numbersCG.add("j");
		numbersCG.add("k");
		numbersCG.add("l");
		// numbersCG.remove(1);
		System.out.println(numbersCG.get(2));
	}

}
