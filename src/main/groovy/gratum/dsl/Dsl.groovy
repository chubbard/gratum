package gratum.dsl

class Dsl {

    static List<GroupDsl> groups = []

    static GroupDsl group( final @DelegatesTo(GroupDsl) Closure closure ) {
        GroupDsl group = new GroupDsl()
        groups << group

        closure.resolveStrategy = Closure.DELEGATE_ONLY
        closure.delegate = group
        closure.call()
        group.execute()
        return group
    }
}
