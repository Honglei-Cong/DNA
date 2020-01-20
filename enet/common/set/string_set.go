// SPDX-License-Identifier: LGPL-3.0-or-later
// Copyright 2019 DNA Dev team
//
/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 *
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package set

import (
	"reflect"
)

// Set is a set of strings, implemented via map[string]struct{} for minimal memory consumption.
type Set map[interface{}]struct{}

// NewSet creates a Set from a list of values.
func NewSet(items ...interface{}) Set {
	ss := Set{}
	ss.Insert(items...)
	return ss
}

// Insert adds items to the set.
func (s Set) Insert(items ...interface{}) Set {
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

// Delete removes all items from the set.
func (s Set) Delete(items ...interface{}) Set {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Set) Has(item interface{}) bool {
	_, contained := s[item]
	return contained
}

// Len returns the size of the set.
func (s Set) Len() int {
	return len(s)
}
